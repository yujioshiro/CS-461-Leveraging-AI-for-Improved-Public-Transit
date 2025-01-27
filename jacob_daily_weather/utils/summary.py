def print_summary_metrics(combined_df, filtered_df, filled_df):
    print("Summary Metrics:")
    print(f"Total Records: {len(combined_df)}")
    print(f"Start Date: {filled_df['DATE'].min()}")
    print(f"End Date: {filled_df['DATE'].max()}")
    print(
        "Missing Max Temp Count: "
        f"{filtered_df['max_temperature'].isna().sum()}"
    )
    print(
        "Missing Min Temp Count: "
        f"{filtered_df['min_temperature'].isna().sum()}"
    )
