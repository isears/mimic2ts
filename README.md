# MIMIC to Time Series

Data processing tool to convert tabular data from [MIMIC IV](https://mimic.mit.edu/docs/) to timeseries data.

## Install

```bash
git clone https://github.com/isears/mimic2ts
cd mimic2ts
pip install ./
```

## Run

```bash
python -m mimic2ts -h
```
```
usage: __main__.py [-h] --src SRC --dst DST [--timestep TIMESTEP]

Convert MIMIC IV tabular data to time series matrices

optional arguments:
  -h, --help           show this help message and exit
  --src SRC            path to the MIMIC IV database
  --dst DST            path to a writeable directory for outputs
  --timestep TIMESTEP  Timeseries timestep value (in seconds)
```