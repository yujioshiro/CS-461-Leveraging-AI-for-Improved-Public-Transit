import os
from utils.general.args import create_parser, handle_paths
from utils.aqi.core import AQIDownloader


def main():
    parser = create_parser("Download AQI Data", category="AQI")
    parser.add_argument("--start-year", type=int, default=2014)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    parser.add_argument("--max-threads", type=int, default=50)
    parser.add_argument("--max-processes", type=int, default=4)
    args = parser.parse_args()
    args = handle_paths(args, category="AQI")

    # Override output_dir to use UTF-8 folder
    if args.test:
        args.output_dir = os.path.join(
            "tests", "data", "sources", "AQI", "utf-8"
        )
        args.start_year = 2024
        args.end_year = 2024
        args.start_month = 12
    else:
        args.output_dir = os.path.join("data", "sources", "AQI", "utf-8")

    try:
        downloader = AQIDownloader(
            args.input_dir, args.output_dir,
            args.start_year, args.end_year,
            args.start_month, args.end_month,
            args.max_threads, args.max_processes
        )
        downloader.execute()
        print("Successfully downloaded and converted AQI data to "
              f"{args.output_dir}")
    except Exception as e:
        print(f"Error in AQI download: {str(e)}")
        raise


if __name__ == "__main__":
    main()
