import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
from typing import List

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

    def _feature_id_parser(self, row):
        return int(row["itemid"])  # Sane default for chartevents, labevents, etc.

    def _value_parser(self, row):
        return float(row["valuenum"])  # Sane default for chartevents

    def _stime_parser(self, row):
        raise NotImplementedError

    def _etime_parser(self, row):
        raise NotImplementedError

    def _handle_feature_group(self, feature_group, tidx_max):
        feature_group["tidx"] = feature_group.apply(
            lambda row: range(row["start_tidx"], row["end_tidx"] + 1), axis=1
        )
        feature_group = feature_group.explode("tidx")
        tidx_grouped = feature_group[["tidx", "value"]].groupby("tidx").mean()
        tidx_grouped = tidx_grouped.reindex(index=range(0, tidx_max + 1), method=None)
        tidx_grouped["value"] = tidx_grouped["value"].fillna(
            0.0
        )  # fill in missing values w/0.0

        return tidx_grouped["value"]

    def _handle_stay_group(self, stay_group):
        stay_id = int(stay_group.name)

        intime = self.icustays["intime"].loc[stay_id]
        tidx_max = self.icustays["total_windows"].loc[stay_id]

        # Convert to epoch seconds
        stay_group["start_tidx"] = (
            stay_group["stime"].values.astype(np.int64) // 10**9
        )
        stay_group["end_tidx"] = stay_group["etime"].values.astype(np.int64) // 10**9
        # ... and then to # of timesteps since icu stay start
        stay_group["start_tidx"] = np.floor_divide(
            (stay_group["start_tidx"] - intime), self.timestep_seconds
        )
        stay_group["end_tidx"] = np.floor_divide(
            (stay_group["end_tidx"] - intime), self.timestep_seconds
        )

        # Consider any measures taken before the official start of the icu stay
        # as taken at tidx 0
        stay_group["start_tidx"] = stay_group["start_tidx"].apply(
            lambda x: x if x > 0 else 0
        )
        stay_group["end_tidx"] = stay_group["end_tidx"].apply(
            lambda x: x if x > 0 else 0
        )

        # Drop any measures taken after the official end of the icu stay
        stay_group = stay_group[stay_group["start_tidx"] <= tidx_max]

        stay_groups_by_featureid = stay_group.groupby("feature_id")
        by_feature = stay_groups_by_featureid.apply(
            self._handle_feature_group, tidx_max=tidx_max
        )
        by_feature.to_csv(f"{self.dst_path}/{stay_id}/{self.name}_features.csv")

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
        # Standardize the format before doing any computation
        self.data["feature_id"] = self.data.apply(
            self._feature_id_parser, axis=1, meta=("int")
        )

        # self._do_filter() # TODO: this may be slowing things down
        self.data = self.data[self.data["stay_id"].isin(self.stay_ids)]

        self.data["stime"] = dd.to_datetime(
            self.data.apply(self._stime_parser, axis=1, meta=("str"))
        )
        self.data["etime"] = dd.to_datetime(
            self.data.apply(self._etime_parser, axis=1, meta=("str"))
        )
        self.data["value"] = self.data.apply(self._value_parser, axis=1, meta=("float"))

        # Do the agg
        self.data.groupby("stay_id").apply(self._handle_stay_group, meta=()).compute(
            scheduler="processes", num_workers=self.cores_available
        )

        for sid in self.stay_ids:
            if not os.path.exists(f"{self.dst_path}/{sid}/{self.name}_features.csv"):
                pd.DataFrame(columns=self.data.columns).to_csv(
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

    def _stime_parser(self, row):
        return row["charttime"]

    def _etime_parser(self, row):
        return row["charttime"]


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

    def _stime_parser(self, row):
        return row["starttime"]

    def _etime_parser(self, row):
        return row["endtime"]

    def _value_parser(self, row):
        # TODO: handle this divide by 0 more gracefully
        if (row["etime"] - row["stime"]).total_seconds() < self.timestep_seconds:
            return row["amount"]
        else:
            return (
                row["amount"] / (row["etime"] - row["stime"]).total_seconds()
            ) / row["patientweight"]


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

    def _stime_parser(self, row):
        return row["charttime"]

    def _etime_parser(self, row):
        return row["charttime"]

    def _value_parser(self, row):
        return float(row["value"])


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

    def do_agg(self):
        for aggregator in self.aggregators:
            aggregator.do_agg()
