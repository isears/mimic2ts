import unittest
import os
import pandas as pd
import shutil

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

        if os.getenv("SHORTTEST") is None:
            print("[*] Running long version of test")
        else:
            print("[*] Running short version of test")
            cls.test_stay_ids = cls.test_stay_ids[0:20]

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
        pass


if __name__ == "__main__":
    unittest.main()
