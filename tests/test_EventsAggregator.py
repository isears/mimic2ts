import unittest
import os
import pandas as pd
import shutil
from datetime import datetime
import numpy as np

# Import without packaging?
from src.mimic2ts import EventsAggregator


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

        if os.getenv("SHORTTEST") is None:
            print("[*] Running long version of test")
        else:
            print("[*] Running short version of test")
            cls.test_stay_ids = cls.test_stay_ids[0:10]

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
        assert len(all_dirs) == len(self.test_stay_ids)

        for dirname in all_dirs:
            fnames = [fname for fname in os.listdir(f"./testcache/{dirname}")]

            assert len(fnames) == 3
            assert "chartevents_features.csv" in fnames
            assert "inputevents_features.csv" in fnames
            assert "outputevents_features.csv" in fnames

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
            ]

            for f_df in feature_dataframes:
                df = pd.read_csv(f_df)

                assert len(df.columns) - 1 == total_timesteps, (
                    f"Timestep mismatch for sid {sid}: "
                    f"expected {total_timesteps}, got {len(df.columns) - 1}"
                )

    def test_event_has_seq_entry(self):
        """
        Test that every event recorded during the icu stay in MIMIC has a corresponding
        entry in the output timeseries sequence
        """

        def assert_event_represented(row, aggregator):
            stay_id = row["stay_id"]
            feature_id = aggregator._feature_id_parser(row)
            # original_value = aggregator._value_parser(row)

            output_df = pd.read_csv(
                f"testcache/{stay_id}/{aggregator.name}_features.csv"
            )

            feature_time_series = output_df[output_df["feature_id"] == feature_id]
            assert len(feature_time_series) == 1
            # Drop first column b/c it's just the feature id
            feature_time_series = feature_time_series.iloc[0].to_list()[1:]

            icustay_row = self.icustays[self.icustays["stay_id"] == stay_id].iloc[0]
            intime = TestEventsAggregator._convert_mimic_date_str_to_epoch(
                icustay_row["intime"]
            )

            stime = TestEventsAggregator._convert_mimic_date_str_to_epoch(
                aggregator._stime_parser(row)
            )
            etime = TestEventsAggregator._convert_mimic_date_str_to_epoch(
                aggregator._etime_parser(row)
            )

            start_tidx = (stime - intime) // self.timestep_seconds
            end_tidx = (etime - intime) // self.timestep_seconds

            assert start_tidx >= 0 and start_tidx < len(feature_time_series), (
                f"Invalid start index {start_tidx} for stay id {stay_id} and "
                f"feature id {feature_id}"
            )
            assert end_tidx >= 0 and end_tidx < len(feature_time_series), (
                f"Invalid end index {end_tidx} for stay id {stay_id} and "
                f"feature id {feature_id}"
            )

            # Can't test value b/c several values may be averaged for one timestep

        for aggregator in self.ea.aggregators:
            original_df = pd.read_csv(f"testmimic/icu/{aggregator.name}.csv")
            original_df = original_df[original_df["stay_id"].isin(self.test_stay_ids)]
            original_df["stime"] = original_df.apply(aggregator._stime_parser, axis=1)
            original_df["etime"] = original_df.apply(aggregator._etime_parser, axis=1)
            original_df = original_df.merge(
                self.icustays[["stay_id", "intime", "outtime"]],
                how="left",
                on="stay_id",
            )
            original_df[["stime", "etime", "intime", "outtime"]] = original_df[
                ["stime", "etime", "intime", "outtime"]
            ].apply(pd.to_datetime)
            original_df = original_df[original_df["stime"] > original_df["intime"]]
            original_df = original_df[original_df["etime"] < original_df["outtime"]]
            original_df.apply(
                lambda row: assert_event_represented(row, aggregator), axis=1
            )


if __name__ == "__main__":
    unittest.main()
