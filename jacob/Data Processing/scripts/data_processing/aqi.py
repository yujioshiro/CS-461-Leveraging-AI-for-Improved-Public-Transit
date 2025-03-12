import os
from utils.general.args import create_parser, handle_paths
from utils.aqi.core import AQIProcessor


def main():
    parser = create_parser("Process AQI Data", category="AQI")
    parser.add_argument("--city", default="Eugene - Highway 99",
                        help="Target city for AQI filtering")
    args = parser.parse_args()
    args = handle_paths(args, category="AQI")

    if args.test:
        args.input_dir = os.path.join(
            "tests", "data", "sources", "AQI", "utf-8"
        )
    else:
        args.input_dir = os.path.join("data", "sources", "AQI", "utf-8")

    try:
        processor = AQIProcessor(args.input_dir, args.output_dir)
        processor.city_filter = args.city
        processor.process()
    except Exception as e:
        print(f"Error processing AQI data: {str(e)}")
        raise


if __name__ == "__main__":
    main()
