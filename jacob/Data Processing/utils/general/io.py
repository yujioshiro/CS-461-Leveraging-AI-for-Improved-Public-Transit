import os
import pandas as pd


def read_files(input_dir, pattern="*.tsv"):
    """Read multiple files into DataFrames"""
    from glob import glob
    return [
        pd.read_csv(f, sep='\t' if f.endswith('.tsv')
                    else '|' if f.endswith('.dat')
                    else ',')
        for f in glob(os.path.join(input_dir, pattern))
    ]


def save_data(df, output_path, sep='\t'):
    """Save DataFrame with auto-created parent dirs"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, sep=sep, index=False)
    print(f"Saved {output_path}")


def read_combine_csvs(input_dir):
    """Combine multiple CSVs/TSVs into single DataFrame"""
    files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith(('.csv', '.tsv'))
    ]

    if not files:
        raise ValueError(f"No CSV/TSV files found in {input_dir}")

    return pd.concat(
        [pd.read_csv(f) for f in files],
        ignore_index=True
    )
