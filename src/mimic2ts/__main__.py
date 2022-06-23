from mimic2ts import EventsAggregator
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert MIMIC IV tabular data to time series matrices"
    )

    parser.add_argument(
        "--src", type=str, required=True, help="path to the MIMIC IV database"
    )

    parser.add_argument(
        "--dst",
        type=str,
        required=True,
        help="path to a writeable directory for outputs",
    )

    parser.add_argument(
        "--exclude",
        type=str,
        required=False,
        help="comma seperated list of datasources to exclude. E.g. '--exclude chartevents,outputevents'",
    )

    parser.add_argument(
        "--timestep",
        type=int,
        required=False,
        help="Timeseries timestep value (in seconds)",
        default=3600,
    )

    args = parser.parse_args()

    excluded_sources = dict()

    for excluded_source in args.exclude.split(","):
        excluded_sources[excluded_source.strip()] = False

    ea = EventsAggregator(
        mimic_path=args.src,
        dst_path=args.dst,
        stay_ids=None,
        feature_ids=None,
        timestep_seconds=args.timestep,
        **excluded_sources,
    )

    print(f"Running aggregator with {ea.aggregators[0].cores_available} processes")
    ea.do_agg()
