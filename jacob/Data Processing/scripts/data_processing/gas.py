import os
import pandas as pd
from utils.general.args import create_parser, handle_paths
from utils.general.io import read_files, save_data
from utils.general.processing import process_date_columns, rename_and_drop

COLUMN_RENAME_MAP = {
    'Date': 'DATE',
    'Weekly West Coast (PADD 5) Except California All Grades All Formulations '
    'Retail Gasoline Prices  (Dollars per Gallon)': 'Weekly Gas Price'
}


def process_gas_prices(input_dir, output_dir):
    """Process gas price data using utility functions"""
    # Read files using io utility
    try:
        dfs = read_files(input_dir)
    except (FileNotFoundError, ValueError) as e:
        raise FileNotFoundError(f"No TSV files found in {input_dir}") from e

    # Combine and process data
    combined_df = pd.concat(dfs, ignore_index=True)

    # Use rename_and_drop even if not dropping columns
    renamed_df = rename_and_drop(
        df=combined_df,
        rename_map=COLUMN_RENAME_MAP,
        drop_cols=[]  # Explicit empty list for future compatibility
    )

    # Process dates using standardized function
    processed_df = process_date_columns(
        renamed_df,
        date_format='%b %d, %Y',
        date_col='DATE'
    )

    # Save using io utility
    output_path = os.path.join(output_dir, "processed_gas.tsv")
    save_data(processed_df, output_path)

    return output_path


if __name__ == "__main__":
    # Configure argument parsing
    parser = create_parser("Process Gas Prices", category="Gas")
    args = parser.parse_args()
    args = handle_paths(args, category="Gas")

    try:
        result_path = process_gas_prices(args.input_dir, args.output_dir)
        print(f"Successfully processed gas data to {result_path}")
    except Exception as e:
        print(f"Error processing gas data: {str(e)}")
        raise
