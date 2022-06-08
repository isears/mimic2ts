"""
Generates test data that is a subset of MIMIC
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
    actual_mimic_path = sys.argv[1]

    test_stay_ids = (
        pd.read_csv("tests/test_stay_ids.csv")["stay_id"].astype(int).to_list()
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
    ]

    for ds in filter_datasources:
        print(f"Filtering {ds}")

        data = dd.read_csv(
            f"{actual_mimic_path}/{ds}",
            assume_missing=True,
            blocksize=10e6,
            dtype=all_inclusive_dtypes,
        )

        data = data[data["stay_id"].isin(test_stay_ids)].compute(scheduler="processes")
        data.to_csv(f"./testmimic/{ds}", index=False)

    copy_datasources = ["icu/d_items.csv"]

    for ds in copy_datasources:
        print(f"Copying {ds}")
        shutil.copy(f"{actual_mimic_path}/{ds}", f"./testmimic/{ds}")
