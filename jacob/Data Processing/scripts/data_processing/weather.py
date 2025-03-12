from utils.general.args import create_parser, handle_paths
from utils.weather.core import WeatherProcessor


def main():
    # Create category-specific parser
    parser = create_parser("Process NOAA weather data", category="Weather")
    args = parser.parse_args()

    try:
        args = handle_paths(args, category="Weather")
        processor = WeatherProcessor(args.input_dir, args.output_dir)
        processor.process()
    except Exception as e:
        print(f"Error processing weather data: {str(e)}")
        raise


if __name__ == "__main__":
    main()
