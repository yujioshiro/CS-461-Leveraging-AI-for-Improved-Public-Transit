import argparse
from utils.io import read_combine_csvs, save_weather_data
from utils.summary import print_summary_metrics
from utils.processing import (
    rename_columns, filter_station, fill_missing_values,
    cast_boolean_columns, date_to_datetime
)


def process_weather_data(input_dir, output_dir):
    # Read, combine, rename, filter, and fill missing values
    combined_df = read_combine_csvs(input_dir)
    combined_df = rename_columns(combined_df)
    filtered_df = filter_station(combined_df)
    filled_df = fill_missing_values(filtered_df)

    # Cast columns, convert dates, save data, and print summary metrics
    cast_boolean_columns(filled_df)
    date_to_datetime(filled_df)
    save_weather_data(filled_df, output_dir)
    print_summary_metrics(combined_df, filtered_df, filled_df)


def main():
    parser = argparse.ArgumentParser(
        description="Process NOAA weather data CSV files."
    )
    parser.add_argument(
        'input_dir', type=str, help="Directory containing input CSV files."
    )
    parser.add_argument(
        'output_dir', type=str, help="Directory for output processed files."
    )
    args = parser.parse_args()

    process_weather_data(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
