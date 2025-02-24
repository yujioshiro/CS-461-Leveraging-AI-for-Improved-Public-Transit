import os


def check_missing_values(df):
    """
    Checks for missing values in the DataFrame and
    prints the amount of missing values for each column.
    """
    missing_values = df.isnull().sum()
    if missing_values.any():
        print("\nAmount of missing values in the combined data:")
        print(missing_values)
    else:
        print("\nNo missing values found in the combined data.")


def save_data(df, output_path):
    """
    Save DataFrame to a file.
    """
    df.to_csv(output_path, sep='\t', index=False)
    print(f"\nCombined data saved to: {output_path}")


def create_output_path(filename, subdir):
    """
    Create an output path for filename in subdirectory.
    """
    return os.path.join(os.path.dirname(__file__), subdir, filename)
