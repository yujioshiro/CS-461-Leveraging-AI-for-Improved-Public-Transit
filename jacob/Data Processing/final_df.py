import pandas as pd
from functools import reduce
import glob
import os
from utils.add_columns import add_holiday_column, add_weekend_column
from utils.data_utils import (
    check_missing_values, save_data, create_output_path
)


def read_file(file_path):
    """
    Read a CSV or TSV file and return a DataFrame.
    Sets the DATE column as the index.
    """
    if file_path.endswith('.tsv'):
        sep = '\t'
    else:  # .csv
        sep = ','

    df = pd.read_csv(file_path, sep=sep, parse_dates=['DATE'])

    return df.set_index('DATE')


def read_and_merge_data(data_dirs):
    """
    Read and merge data files from a list of directories.
    """
    file_paths = []
    for directory in data_dirs:
        full_dir = os.path.join(os.path.dirname(__file__), directory)
        file_paths.extend(glob.glob(os.path.join(full_dir, '*.tsv')))

    if not file_paths:
        raise FileNotFoundError(f"No CSV/TSV files found in: {data_dirs}")

    print(f"Merging {len(file_paths)} files from:")
    print("\n".join(data_dirs))
    print("Files found:\n-", "\n- ".join(file_paths))

    # Separate out gas price file paths
    # Since gas price comes in a weekly format
    gas_file_paths = [fp for fp in file_paths if 'Gas' in fp]
    other_file_paths = [fp for fp in file_paths if 'Gas' not in fp]

    # Read other data files
    dataframes = [read_file(fp) for fp in other_file_paths]

    # Merge dataframes and preserve all dates
    combined_df = reduce(
        lambda left, right: pd.merge(
            left,
            right,
            left_index=True,
            right_index=True,
            how='outer'
        ),
        dataframes
    )

    return combined_df, gas_file_paths


def process_gas_data(gas_file_paths):
    """
    Read and merge gas price data.
    """
    gas_dfs = [read_file(fp) for fp in gas_file_paths]
    gas_df = pd.concat(gas_dfs).reset_index()
    return gas_df


def main():
    # Omitted the LTD data since I'm not sure if we can share it
    data_dirs = [
        # './data/LTD/processed',     # LTD data
        './data/Weather/processed',   # Weather data
        './data/AQI/processed',       # Air Quality Index data
        './data/Gas/processed',       # Gas price data
        './data/CPI/processed'        # CPI data
    ]

    combined_df, gas_file_paths = read_and_merge_data(data_dirs)
    gas_df = process_gas_data(gas_file_paths)

    # Layer the weekly gas price data onto daily format of the combined data
    combined_df = combined_df.reset_index()
    combined_df = pd.merge_asof(
        combined_df.sort_values('DATE'),
        gas_df.sort_values('DATE'),
        on='DATE',
        direction='backward'
    )

    # Make DATE a column again and sort
    combined_df = combined_df.sort_values('DATE')

    # Forward fill CPI columns
    cpi_columns = [
        col for col in combined_df.columns if col.startswith('CPI:')
    ]
    combined_df[cpi_columns] = combined_df[cpi_columns].ffill()

    # Drop rows with any null values
    combined_df = combined_df.dropna()

    # Add 'Is Weekend' and 'Is Holiday' columns
    combined_df = add_weekend_column(combined_df)
    combined_df = add_holiday_column(combined_df)

    output_path = create_output_path(
        'combined_data.tsv', '../data/Final'
    )

    save_data(combined_df, output_path)
    check_missing_values(combined_df)


if __name__ == "__main__":
    main()
