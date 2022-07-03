import unittest
import os
import pandas as pd
import shutil
from datetime import datetime
import numpy as np
from mimic2ts import EventsAggregator


class TestEventsAggregator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        shutil.rmtree("./testcache")
        cls.test_stay_ids = (
            # Only taking the first 10 for fast tests
            pd.read_csv("tests/test_stay_ids.csv")["stay_id"]
            .astype(int)
            .to_list()
        )

        cls.icustays = pd.read_csv("testmimic/icu/icustays.csv")
        cls.inputevents = pd.read_csv("testmimic/icu/inputevents.csv")
        cls.outputevents = pd.read_csv("testmimic/icu/outputevents.csv")
        cls.chartevents = pd.read_csv("testmimic/icu/chartevents.csv")
        cls.procedureevents = pd.read_csv("testmimic/icu/procedureevents.csv")

        cls.test_feature_ids = pd.read_csv("tests/test_feature_ids.csv")[
            "feature_id"
        ].to_list()

        cls.timestep_seconds = 3600
        cls.ea = EventsAggregator(
            mimic_path="./testmimic",
            dst_path="./testcache",
            stay_ids=cls.test_stay_ids,
            feature_ids=cls.test_feature_ids,
            timestep_seconds=cls.timestep_seconds,
        )

        cls.ea.do_agg()

    def _convert_mimic_date_str_to_epoch(s):
        time_0 = datetime(1970, 1, 1)
        time_in = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return int((time_in - time_0).total_seconds())

    def test_smoketest(self):
        """
        Just test if the aggregator object instantiates
        """
        assert self.ea.timestep_seconds == self.timestep_seconds

    def test_dst_dir_struct(self):
        """
        Test that the destination directory has all the folders and files it should
        """

        all_dirs = [dirname for dirname in os.listdir("./testcache")]
        # Can no longer guarantee that a directory will be produced for each stay id
        # Certain stay ids don't have any events recorded for the icu stay
        # assert len(all_dirs) == len(self.test_stay_ids)

        for dirname in all_dirs:
            fnames = [fname for fname in os.listdir(f"./testcache/{dirname}")]

            assert len(fnames) == 4
            assert "chartevents_features.csv" in fnames
            assert "inputevents_features.csv" in fnames
            assert "outputevents_features.csv" in fnames
            assert "procedureevents_features.csv" in fnames

    def test_has_correct_seq_len(self):
        """
        Test that all entries have the correct sequence length based on
        intime / outtime / timestep size
        """

        for sid in self.test_stay_ids:
            icustay_row = self.icustays[self.icustays["stay_id"] == sid]
            assert len(icustay_row) == 1
            icustay_row = icustay_row.iloc[0]

            intime = TestEventsAggregator._convert_mimic_date_str_to_epoch(
                icustay_row["intime"]
            )
            outtime = TestEventsAggregator._convert_mimic_date_str_to_epoch(
                icustay_row["outtime"]
            )

            total_timesteps = int(np.ceil((outtime - intime) / self.timestep_seconds))

            feature_dataframes = [
                f"testcache/{sid}/chartevents_features.csv",
                f"testcache/{sid}/inputevents_features.csv",
                f"testcache/{sid}/outputevents_features.csv",
                f"testcache/{sid}/procedureevents_features.csv",
            ]

            for f_df in feature_dataframes:
                df = pd.read_csv(f_df)

                assert len(df.columns) - 1 == total_timesteps, (
                    f"Timestep mismatch for sid {sid}: "
                    f"expected {total_timesteps}, got {len(df.columns) - 1}"
                )

    def test_numerical_chartevent_close(self):
        """
        Tests that the average of all numerical chartevents for a given feature is within
        10% of the average of the chartevents in the aggregated time series.
        The averages may not be exactly equal b/c if there are several measurements in one
        time window they will appear as multiple values in the original chartevents, but
        will be a single value in the corresponding aggregated timeseries.
        """
        # dropping all chartevents with a value of 0.0 b/c 0.0 is used as the "filler" value during agg
        nonzero_ce = self.chartevents[
            (self.chartevents["valuenum"] != 0.0)
            & (~self.chartevents["valuenum"].isna())
        ]

        averaged_chartevents = nonzero_ce.groupby(["stay_id", "itemid"]).apply(
            lambda g: (g["valuenum"].astype("float")).mean()
        )

        stay_ids_with_chartevents = nonzero_ce["stay_id"].unique()

        for sid in stay_ids_with_chartevents:
            aggregated_chartevents = pd.read_csv(
                f"testcache/{sid}/chartevents_features.csv", index_col=0
            )

            curr_sid_averaged_chartevents = averaged_chartevents.loc[sid]

            for feature_id in curr_sid_averaged_chartevents.index.to_list():
                curr_feature_timeseries = aggregated_chartevents.loc[feature_id]

                # Need to drop 0.0s because they are "filler" values during agg
                curr_feature_timeseries = curr_feature_timeseries[
                    curr_feature_timeseries != 0.0
                ]

                # Averages will not be exact for time window settings that are larger than
                # the smallest time between chartevent measurements,
                # so just testing if within 50%
                actual = curr_feature_timeseries.mean()
                desired = curr_sid_averaged_chartevents.loc[feature_id]

                # It's ok to be nan as long as both are nan
                if np.isnan(desired):
                    assert np.isnan(actual)
                else:
                    assert np.isclose(
                        actual,
                        desired,
                        rtol=0.5,
                    ), f"[-] Isclose test failed for feature id {feature_id} and stay id {sid}: desired {desired}, actual {actual}"

    def test_summed_events_conserved(self):
        """
        Test that events that are aggregated by the "sum" function are conserved
        between the original data and the aggregated version
        """
        for summed_datasource in [
            "inputevents",
            "outputevents",
            "procedureevents",
        ]:
            df = getattr(self, summed_datasource)

            if summed_datasource in ["outputevents", "procedureevents"]:
                totals = df.groupby(["stay_id", "itemid"]).apply(
                    lambda g: (g["value"].astype("float")).sum()
                )
            elif summed_datasource in ["inputevents"]:
                totals = df.groupby(["stay_id", "itemid"]).apply(
                    lambda g: (
                        g["amount"].astype("float") / g["patientweight"].astype("float")
                    ).sum()
                )

            else:
                raise RuntimeError("Increased datasources without appropriate refactor")

            stay_ids_with_events = df["stay_id"].unique()

            for sid in stay_ids_with_events:
                aggregated_events = pd.read_csv(
                    f"testcache/{sid}/{summed_datasource}_features.csv", index_col=0
                )
                aggregated_events["sum"] = aggregated_events.sum(axis=1)
                totals_by_type = totals.loc[sid]

                for feature_id in totals_by_type.index.to_list():
                    actual = totals_by_type.loc[feature_id]
                    desired = aggregated_events["sum"].loc[feature_id]
                    assert np.isclose(
                        actual,
                        desired,
                        rtol=0.1,
                    ), f"[-] Isclose test failed for feature id {feature_id} and stay id {sid}: desired {desired}, actual {actual}"


if __name__ == "__main__":
    unittest.main()
