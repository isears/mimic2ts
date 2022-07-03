# MIMIC to Time Series

Data processing tool to convert tabular data from [MIMIC IV](https://mimic.mit.edu/docs/) to timeseries data.

NB: This tools has not been updated for MIMIC IV v1.0. It has only been tested on MIMIC IV v0.4.

## Install

For normal usage:
```bash
git clone https://github.com/isears/mimic2ts
cd mimic2ts
pip install ./
```

For debugging and running tests:
```bash
pip install --editable ./
```

## Run

```bash
python -m mimic2ts -h
```
```
usage: __main__.py [-h] [--exclude EXCLUDE] [--timestep TIMESTEP] [--blocksize BLOCKSIZE] src dst

Convert MIMIC IV tabular data to time series matrices

positional arguments:
  src                   path to the MIMIC IV database
  dst                   path to a writeable directory for outputs

optional arguments:
  -h, --help            show this help message and exit
  --exclude EXCLUDE     comma seperated list of datasources to exclude. E.g. '--exclude chartevents,outputevents'
  --timestep TIMESTEP   Timeseries timestep value (in seconds)
  --blocksize BLOCKSIZE
                        Dask blocksize: bigger is faster for a single worker but smaller means more workers can participate
```

## Expected Results

The script should take tabular representations of data from the original MIMIC IV csv files and turn it into a directory structure with the following format:

```
destination_directory/
  30419490/
    chartevents_features.csv
    inputevents_features.csv
    outputevents_features.csv
  31055829/
    chartevents_features.csv
    inputevents_features.csv
    outputevents_features.csv
  31175209/
    chartevents_features.csv
    inputevents_features.csv
    outputevents_features.csv
  ...
  ...
```

Each directory is named for a specific `stay_id` in MIMIC. Each of the csv files contains a table with the following format:

| feature_id | 0   | 1  | ... | n  |
|------------|-----|----|-----|----|
| 220045     | 112 | 90 |     | 92 |
| 220210     | 14  | 11 |     | 12 |
| ...        |     |    |     |    |

Along the x-axis are timesteps where 0 is the first time window during the icu stay and n is the last time window during the icu stay. Along the y-axis are various features such as heart rate, respiration rate, etc.

When there are many measurements of the same feature during a given time step, they are averaged together. When there are no measurements of the feature during a given time step, the value is set to 0.0.

Every table within a `stay_id` directory is guaranteed to have the same number of total timesteps (n), however, different tables from two different `stay_id` directories may not due to differing icu stay lengths.

## Sample Pytorch Dataset

The repository includes a sample pytorch dataset to demonstrate loading the processed timeseries data using pytorch dataloaders. Additional data processing steps can be added by extending the functionality of the sample dataset.

To run the demo:
```bash
python tools/sampleDataset.py
```