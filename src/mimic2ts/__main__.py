from mimic2ts import EventsAggregator
import argparse
import datetime

import mimic2ts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert MIMIC IV tabular data to time series matrices"
    )

    parser.add_argument("src", type=str, help="path to the MIMIC IV database")

    parser.add_argument(
        "dst",
        type=str,
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

    parser.add_argument(
        "--blocksize",
        type=str,
        required=False,
        help="Dask blocksize: bigger is faster for a single worker but smaller means more workers can participate",
        default="default",
    )

    args = parser.parse_args()

    excluded_sources = dict()

    if args.exclude:
        for excluded_source in args.exclude.split(","):
            excluded_sources[excluded_source.strip()] = False

    try:
        blocksize = int(args.blocksize)
    except ValueError:
        assert (
            args.blocksize == "default"
        ), f"[-] Error invalid value for blocksize: {args.blocksize}"
        blocksize = args.blocksize

    ea = EventsAggregator(
        mimic_path=args.src,
        dst_path=args.dst,
        stay_ids=None,
        feature_ids=None,
        timestep_seconds=args.timestep,
        blocksize=args.blocksize,
        **excluded_sources,
    )

    print(f"Running aggregator with {ea.aggregators[0].cores_available} processes")
    starttime = datetime.datetime.now()
    ea.do_agg()
    runtime = datetime.datetime.now() - starttime

    arg_str = "\n".join(f"{k}={v}" for k, v in vars(args).items())

    with open(f"{args.dst}/readme.txt", "w") as f:
        f.write(
            f"Mimic2ts version {mimic2ts.__version__} aggregation completed on {datetime.datetime.now()}\n"
        )
        f.write(f"Runtime: {str(runtime)}\n")
        f.write("Arguments:\n")
        f.write(arg_str)
