import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import os
from typing import List
from mimic2ts.version import __version__

all_inclusive_dtypes = {
    # Chartevents
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "charttime": "object",
    "storetime": "object",
    "itemid": "int",
    "value": "object",
    "valueuom": "object",
    "warning": "object",
    "valuenum": "float",
    # Inputevents
    "starttime": "object",
    "endtime": "object",
    "amount": "float",
    "amountuom": "object",
    "rate": "float",
    "rateuom": "object",
    "orderid": "int",
    "linkorderid": "int",
    "ordercategoryname": "object",
    "secondaryordercategoryname": "object",
    "ordercomponenttypedescription": "object",
    "ordercategorydescription": "object",
    "patientweight": "float",
    "totalamount": "float",
    "totalamountuom": "object",
    "isopenbag": "int",
    "continueinnextdept": "int",
    "cancelreason": "int",
    "statusdescription": "object",
    "originalamount": "float",
    "originalrate": "float",
}


class BaseAggregator(object):
    def __init__(
        self,
        mimic_path: str,
        dst_path: str,
        stay_ids: List[int],
        feature_ids: List[int],
        timestep_seconds: int,
        name: str,
    ):
        self.stay_ids = stay_ids
        self.feature_ids = feature_ids
        self.mimic_path = mimic_path
        self.dst_path = dst_path
        self.timestep_seconds = timestep_seconds
        self.name = name
        self.cores_available = len(os.sched_getaffinity(0))

        self.icustays = pd.read_csv(f"{self.mimic_path}/icu/icustays.csv")
        self.d_items = pd.read_csv(f"{self.mimic_path}/icu/d_items.csv")

        if self.stay_ids is None:  # If none given, use all
            self.stay_ids = self.icustays["stay_id"].to_list()

        for sid in self.stay_ids:
            if not os.path.exists(f"{self.dst_path}/{sid}"):
                os.makedirs(f"{self.dst_path}/{sid}")

        def format_dates(x):
            as_dt = pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S")
            as_seconds = as_dt.values.astype(np.int64) // 10**9
            return as_seconds

        self.icustays[["intime", "outtime"]] = self.icustays[
            ["intime", "outtime"]
        ].apply(format_dates)

        self.icustays["total_windows"] = (
            self.icustays["outtime"] - self.icustays["intime"]
        ) // self.timestep_seconds

        self.icustays = self.icustays[["stay_id", "intime", "outtime", "total_windows"]]
        self.icustays = self.icustays.set_index("stay_id")

    def _feature_combiner(self, tidx_group: pd.DataFrame):
        """
        How to combine features when there's many in one time window
        """
        raise NotImplementedError

    def _feature_id_parser(self, row):
        return int(row["itemid"])  # Sane default for chartevents, labevents, etc.

    def _value_parser(self, row):
        raise NotImplementedError

    def _parse_dates(self):
        self.data["event_epoch_time"] = (
            dd.to_datetime(self.data["charttime"]).values.astype(np.int64) // 10**9
        )

    def _handle_feature_group(self, feature_group, tidx_max):
        try:
            tidx_grouped = (
                feature_group[["tidx", "value"]]
                .groupby("tidx")
                .apply(lambda x: self._feature_combiner(x))
            )
            tidx_grouped = tidx_grouped.reindex(
                index=range(0, tidx_max + 1), method=None
            )
            tidx_grouped["value"] = tidx_grouped["value"].fillna(
                0.0
            )  # fill in missing values w/0.0

            return tidx_grouped["value"]
        except Exception as e:
            print(f"The feature id triggering the exception is {feature_group.name}")
            raise e

    def _handle_stay_group(self, stay_group):
        if stay_group.empty:  # Sometimes dask generates empty dataframes
            return

        stay_id = int(stay_group.name)

        try:

            intime = self.icustays["intime"].loc[stay_id]
            tidx_max = self.icustays["total_windows"].loc[stay_id]

            stay_group["tidx"] = (
                np.floor_divide(
                    (stay_group["event_epoch_time"] - intime), self.timestep_seconds
                )
            ).astype("int")

            # Consider any measures taken before the official start of the icu stay
            # as taken at tidx 0
            stay_group["tidx"] = stay_group["tidx"].apply(lambda x: x if x > 0 else 0.0)

            # Drop any measures taken after the official end of the icu stay
            stay_group = stay_group[stay_group["tidx"] <= tidx_max]

            if stay_group.empty:  # Check again in case tdx filtering dropped everything
                return

            stay_groups_by_featureid = stay_group.groupby("feature_id")
            by_feature = stay_groups_by_featureid.apply(
                self._handle_feature_group, tidx_max=tidx_max
            )
            by_feature.to_csv(f"{self.dst_path}/{stay_id}/{self.name}_features.csv")
        except Exception as e:
            print(f"The stay id triggering the exception is {stay_id}")
            raise e

    def _do_filter(self):
        # optimize filtering for large or small numbers of feature / stay ids
        if len(self.stay_ids) > len(self.icustays.index) // 2:
            # Case of very large number of stay ids
            left_out_sids = set(self.stay_ids) - set(self.icustays["stay_id"].to_list())
            self.data = self.data[~self.data["stay_id"].isin(left_out_sids)]
        else:
            self.data = self.data[self.data["stay_id"].isin(self.stay_ids)]

        if len(self.feature_ids) > len(self.d_items.index) // 2:
            left_out_fids = set(self.feature_ids) - set(self.d_items["itemid"])
            self.data = self.data[~self.data["feature_id"].isin(left_out_fids)]
        else:
            self.data = self.data[self.data["feature_id"].isin(self.feature_ids)]

    def do_agg(self):
        print(f"{type(self).__name__}: running aggregation")
        self._parse_dates()

        # Standardize the format before doing any computation
        self.data["feature_id"] = self.data.apply(
            self._feature_id_parser, axis=1, meta=pd.Series([1])
        )

        # self._do_filter() # TODO: this may be slowing things down
        self.data = self.data[self.data["stay_id"].isin(self.stay_ids)]

        self.data["value"] = self.data.apply(
            self._value_parser, axis=1, meta=pd.Series([0.0])
        )

        # Do the agg
        self.data.groupby("stay_id").apply(
            self._handle_stay_group, meta=pd.DataFrame()
        ).compute(scheduler="processes", num_workers=self.cores_available)

        # Make dummy dataframes for anything that doesn't have data
        for sid in self.stay_ids:
            if not os.path.exists(f"{self.dst_path}/{sid}/{self.name}_features.csv"):
                cols = ["feature_id"] + list(
                    range(0, self.icustays["total_windows"].loc[sid] + 1)
                )
                pd.DataFrame(columns=cols).to_csv(
                    f"{self.dst_path}/{sid}/{self.name}_features.csv", index=False
                )


class ChartEventAggregator(BaseAggregator):
    def __init__(
        self,
        mimic_path: str,
        dst_path: str,
        stay_ids: List[int],
        feature_ids: List[int],
        timestep_seconds: int = 3600,
        blocksize=10e6,
    ):

        self.data = dd.read_csv(
            f"{mimic_path}/icu/chartevents.csv",
            assume_missing=True,
            blocksize=blocksize,
            dtype=all_inclusive_dtypes,
        )

        super().__init__(
            mimic_path, dst_path, stay_ids, feature_ids, timestep_seconds, "chartevents"
        )

    def _value_parser(self, row):
        return float(row["valuenum"])

    def _feature_combiner(self, tidx_group: pd.DataFrame):
        return tidx_group.mean()


class InputEventAggregator(BaseAggregator):
    def __init__(
        self,
        mimic_path: str,
        dst_path: str,
        stay_ids: List[int],
        feature_ids: List[int],
        timestep_seconds: int = 3600,
        blocksize=10e6,
    ):

        self.data = dd.read_csv(
            f"{mimic_path}/icu/inputevents.csv",
            assume_missing=True,
            blocksize=blocksize,
            dtype=all_inclusive_dtypes,
        )

        super().__init__(
            mimic_path, dst_path, stay_ids, feature_ids, timestep_seconds, "inputevents"
        )

    def _value_parser(self, row):
        return row["amount"] / row["patientweight"]

    def _feature_combiner(self, tidx_group: pd.DataFrame):
        return tidx_group.sum()

    def _parse_dates(self):
        self.data["start_epoch_time"] = (
            dd.to_datetime(self.data["starttime"]).values.astype(np.int64) // 10**9
        )
        self.data["end_epoch_time"] = (
            dd.to_datetime(self.data["endtime"]).values.astype(np.int64) // 10**9
        )

    def _handle_stay_group(self, stay_group):
        """
        Specialized method to handle stay groups for inputevents so that
        events can be generated in range between starttime and endtime
        """
        if stay_group.empty:  # Sometimes dask generates empty dataframes
            return

        stay_id = int(stay_group.name)

        try:
            stay_group["event_epoch_time"] = stay_group.apply(
                lambda row: range(
                    row["start_epoch_time"],
                    # Guaranteed to always have one element
                    row["end_epoch_time"] + self.timestep_seconds,
                    self.timestep_seconds,
                ),
                axis=1,
            )

            # Evenly divide the dose over all windows
            stay_group["value"] = stay_group.apply(
                lambda row: row["value"] / len(row["event_epoch_time"]), axis=1
            )

            stay_group = stay_group.explode("event_epoch_time")
            stay_group.name = stay_id  # Give it back so super can read it

        except Exception as e:
            print(f"The stay id triggering the exception is {stay_id}")
            raise e

        super()._handle_stay_group(stay_group)


class OutputEventAggregator(BaseAggregator):
    def __init__(
        self,
        mimic_path: str,
        dst_path: str,
        stay_ids: List[int],
        feature_ids: List[int],
        timestep_seconds: int = 3600,
        blocksize=10e6,
    ):

        self.data = dd.read_csv(
            f"{mimic_path}/icu/outputevents.csv",
            assume_missing=True,
            blocksize=blocksize,
            dtype=all_inclusive_dtypes,
        )

        super().__init__(
            mimic_path,
            dst_path,
            stay_ids,
            feature_ids,
            timestep_seconds,
            "outputevents",
        )

    def _value_parser(self, row):
        return float(row["value"])

    def _feature_combiner(self, tidx_group: pd.DataFrame):
        return tidx_group.sum()


class EventsAggregator(object):
    def __init__(
        self,
        mimic_path: str,
        dst_path: str,
        stay_ids: List[int],
        feature_ids: List[int],
        timestep_seconds: int = 3600,
        chartevents: bool = True,
        inputevents: bool = True,
        outputevents: bool = True,
    ):

        self.mimic_path = mimic_path
        self.dst_path = dst_path
        self.timestep_seconds = timestep_seconds

        self.aggregators = list()

        if chartevents:
            self.aggregators.append(
                ChartEventAggregator(
                    mimic_path,
                    dst_path,
                    stay_ids,
                    feature_ids,
                    timestep_seconds=timestep_seconds,
                )
            )

        if inputevents:
            self.aggregators.append(
                InputEventAggregator(
                    mimic_path,
                    dst_path,
                    stay_ids,
                    feature_ids,
                    timestep_seconds=timestep_seconds,
                )
            )

        if outputevents:
            self.aggregators.append(
                OutputEventAggregator(
                    mimic_path,
                    dst_path,
                    stay_ids,
                    feature_ids,
                    timestep_seconds=timestep_seconds,
                )
            )

        ProgressBar().register()

    def do_agg(self):
        for aggregator in self.aggregators:
            aggregator.do_agg()
