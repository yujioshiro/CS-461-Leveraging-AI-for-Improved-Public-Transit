import os
import pandas as pd


def read_combine_csvs(input_dir):
    # Read and combine all CSVs
    all_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith('.csv')
    ]
    combined_df = pd.concat(
        (
            pd.read_csv(
                f, low_memory=False, dtype={
                    'WT01': 'float', 'WT02': 'float', 'WT03': 'float',
                    'WT04': 'float', 'WT05': 'float', 'WT06': 'float',
                    'WT07': 'float', 'WT08': 'float', 'WT09': 'float',
                    'WT10': 'float', 'WT11': 'float', 'WT12': 'float',
                    'WT13': 'float', 'WT14': 'float', 'WT15': 'float',
                    'WT16': 'float', 'WT17': 'float', 'WT18': 'float',
                    'WT19': 'float', 'WT21': 'float', 'WT22': 'float'
                }
            ) for f in all_files
        ),
        ignore_index=True
    )

    return combined_df


def save_weather_data(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(
        os.path.join(output_dir, 'processed_weather_data.tsv'),
        sep='\t', index=False
    )
    df.to_parquet(
        os.path.join(output_dir, 'processed_weather_data.parquet'),
        index=False
    )
