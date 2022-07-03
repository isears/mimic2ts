"""
Generates test data that is a subset of MIMIC

Usage:
python tools/test_data_generator.py /path/to/real/MIMIC/data
"""
import dask.dataframe as dd
import pandas as pd
import os
import sys
import shutil

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

if __name__ == "__main__":
    if len(sys.argv) == 2:
        actual_mimic_path = sys.argv[1]
    else:
        actual_mimic_path = "./mimiciv"

    test_stay_ids = [
        # "Buggy" stay ids that have revealed problems in the past
        30324975,
        33990872,
        ## stays ids that broke after inputevents refactor
        34909767,
        37533185,
        31738657,
        30720284,
        35601991,
        # Random sample stay ids
        30324975,
        32491957,
        33000507,
        38291278,
        33563199,
    ]

    pd.Series(test_stay_ids, name="stay_id").to_csv(
        "tests/test_stay_ids.csv", index=False
    )

    print(
        f"Filtering data from {actual_mimic_path} down to {len(test_stay_ids)} stay ids"
    )

    if not os.path.exists("./testmimic/icu"):
        os.makedirs("./testmimic/icu")

    filter_datasources = [
        "icu/icustays.csv",
        "icu/chartevents.csv",
        "icu/inputevents.csv",
        "icu/outputevents.csv",
        "icu/procedureevents.csv",
    ]

    for ds in filter_datasources:
        print(f"Filtering {ds}")

        data = dd.read_csv(
            f"{actual_mimic_path}/{ds}",
            assume_missing=True,
            blocksize=1e6,
            dtype=all_inclusive_dtypes,
        )

        data = data[data["stay_id"].isin(test_stay_ids)].compute(scheduler="processes")
        data.to_csv(f"./testmimic/{ds}", index=False)

    copy_datasources = ["icu/d_items.csv"]

    for ds in copy_datasources:
        print(f"Copying {ds}")
        shutil.copy(f"{actual_mimic_path}/{ds}", f"./testmimic/{ds}")
