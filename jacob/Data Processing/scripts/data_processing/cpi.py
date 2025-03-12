import os
import pandas as pd
from utils.general.args import create_parser, handle_paths
from utils.general.processing import process_date_columns, pivot_data
from utils.general.io import save_data, read_files

SERIES_MAPPING = {
    "CUUR0000SA0": "CPI: All items",
    "CUUR0000SAF": "CPI: Food and Beverages",
    "CUUR0000SAF11": "CPI: Food at home",
    "CUUR0000SAF111": "CPI: Cereals and bakery products",
    "CUUR0000SAF112": "CPI: Meats, poultry, fish, and eggs",
    "CUUR0000SAF113": "CPI: Fruits and vegetables",
    "CUUR0000SAF114": "CPI: Nonalcoholic beverages and bev. materials",
    "CUUR0000SAF115": "CPI: Other food at home",
    "CUUR0000SEFJ": "CPI: Dairy and related products",
    "CUUR0000SEFV": "CPI: Food away from home",
    "CUUR0000SEFV01": "CPI: Full service meals and snacks",
    "CUUR0000SEFV02": "CPI: Limited service meals and snacks",
    "CUUR0000SA0E": "CPI: Energy",
    "CUUR0000SETB01": "CPI: Gasoline (all types)",
    "CUUR0000SEHF01": "CPI: Electricity",
    "CUUR0000SEHF02": "CPI: Utility (piped) gas service",
    "CUUR0000SA0L1E": "CPI: All items less food and energy",
    "CUUR0000SAH": "CPI: Housing",
    "CUUR0000SAH1": "CPI: Shelter",
    "CUUR0000SAA": "CPI: Apparel",
    "CUUR0000SAR": "CPI: Recreation",
    "CUUR0000SAE1": "CPI: Education",
    "CUUR0000SAE2": "CPI: Communication",
    "CUUR0000SAM": "CPI: Medical Care",
    "CUUR0000SEMD01": "CPI: Hospital Services",
    "CUUR0000SEMC01": "CPI: Physicians' Services",
    "CUUR0000SEMF01": "CPI: Prescription Drugs",
    "CUUR0000SAT": "CPI: Transportation",
    "CUUR0000SETA01": "CPI: New Vehicles",
    "CUUR0000SETA02": "CPI: Used Cars and Trucks"
}


def process_cpi_data(input_dir, output_dir):
    dfs = read_files(input_dir)

    processed_dfs = []
    for df in dfs:
        # Filter out rows with Period starting with 'S'
        df = df.loc[~df['Period'].str.startswith('S')].copy()

        # Create DATE column with special case handling
        df['DATE'] = pd.to_datetime(
            df['Year'].astype(str) + '-' +
            df['Period'].str[1:] +
            df.apply(lambda row: '-06'
                     if (row['Year'] == 2014 and row['Period'] == 'M01')
                     else '-01', axis=1)
        )

        # Process dates and map series IDs
        df = process_date_columns(df)
        df['Series ID'] = df['Series ID'].map(SERIES_MAPPING)
        df = df.drop(columns=['Year', 'Period'])
        processed_dfs.append(df)

    # Combine and pivot data
    combined_df = pd.concat(processed_dfs, ignore_index=True)
    pivoted_df = pivot_data(combined_df, 'DATE', 'Series ID', 'Value')

    # Save output
    output_path = os.path.join(output_dir, 'processed_cpi_data.tsv')
    save_data(pivoted_df, output_path)


if __name__ == "__main__":
    parser = create_parser("Process CPI Data", category="CPI")
    args = parser.parse_args()
    args = handle_paths(args, category="CPI")
    process_cpi_data(args.input_dir, args.output_dir)
