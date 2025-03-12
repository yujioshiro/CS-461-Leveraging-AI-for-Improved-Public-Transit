import os
import argparse
import glob
import pandas as pd
from utils.general.io import save_data
from utils.general.processing import (
    process_date_columns, add_holiday_column, add_weekend_column
)


def read_all_tsv(data_dir, pattern="*.tsv"):
    """Read and aggregate all TSV files in the specified directory."""
    files = glob.glob(os.path.join(data_dir, pattern))
    if not files:
        raise FileNotFoundError(f"No TSV files found in {data_dir}")
    dfs = [pd.read_csv(file, sep='\t') for file in files]
    return pd.concat(dfs, ignore_index=True)


def read_ltd_data(ltd_dir):
    """Read and aggregate LTD data from all TSV files in the directory."""
    combined = read_all_tsv(ltd_dir)
    return combined.groupby('DATE', as_index=False)['total_board'].sum()


def merge_datasets(data_paths):
    """Merge all processed datasets"""
    # Read all datasets using the helper function
    datasets = {
        'ltd': read_ltd_data(data_paths['ltd']),
        'weather': read_all_tsv(data_paths['weather']),
        'aqi': read_all_tsv(data_paths['aqi']),
        'cpi': read_all_tsv(data_paths['cpi']),
        'gas': read_all_tsv(data_paths['gas']),
    }

    # Standardize date columns for each dataset
    for key in datasets:
        datasets[key] = process_date_columns(datasets[key])
        datasets[key]['DATE'] = pd.to_datetime(datasets[key]['DATE'])

    # Merge core daily datasets
    merged = pd.merge(
        datasets['ltd'], datasets['weather'], on='DATE', how='outer'
    )
    merged = pd.merge(merged, datasets['aqi'], on='DATE', how='outer')

    # Merge and forward-fill monthly CPI data
    merged = pd.merge(merged, datasets['cpi'], on='DATE', how='left')
    cpi_cols = [col for col in datasets['cpi'].columns if col != 'DATE']
    merged[cpi_cols] = merged[cpi_cols].ffill()

    # Merge weekly gas prices using nearest backward lookup
    merged = pd.merge_asof(
        merged.sort_values('DATE'),
        datasets['gas'][['DATE', 'Weekly Gas Price']].sort_values('DATE'),
        on='DATE',
        direction='backward'
    )

    return merged


def main():
    # Configure argument parser
    parser = argparse.ArgumentParser(
        description="Merge all processed datasets into final dataframe"
    )
    parser.add_argument(
        "--test", action="store_true", help="Use test data directories"
    )
    parser.add_argument(
        "--output-dir", default="data/final/processed", help="Output directory"
    )
    args = parser.parse_args()

    # Set up data paths
    base_dir = os.path.join(
        "tests", "data" if args.test else "data", "sources"
    )
    data_paths = {
        'ltd': os.path.join(base_dir, "LTD", "processed"),
        'weather': os.path.join(base_dir, "Weather", "processed"),
        'aqi': os.path.join(base_dir, "AQI", "processed"),
        'gas': os.path.join(base_dir, "Gas", "processed"),
        'cpi': os.path.join(base_dir, "CPI", "processed"),
    }

    # Determine output directory based on --test flag
    if args.test:
        output_dir = os.path.join("tests", "data", "merged")
    else:
        output_dir = args.output_dir

    # Merge datasets
    try:
        final_df = merge_datasets(data_paths)

        # Add temporal features
        final_df = add_holiday_column(final_df)
        final_df = add_weekend_column(final_df)

        # Clean data
        final_df.dropna(inplace=True)
        final_df.sort_values('DATE', inplace=True)

        # Save results
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "final_dataset.tsv")
        save_data(final_df, output_path)
        print(f"Successfully created final dataset at {output_path}")

    except Exception as e:
        print(f"Error merging datasets: {str(e)}")
        raise


if __name__ == "__main__":
    main()
