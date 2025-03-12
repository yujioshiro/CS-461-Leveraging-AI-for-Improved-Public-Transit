import pandas as pd
import holidays


def filter_date(df, date_col):
    """
    Filters the input DataFrame to the desired date range.
    The range has been determined by the earlist common first entry date and
    the latest common last entry date.
    """

    start_date = '2014-01-06'  # Gas Data limitation
    end_date = '2024-12-11'    # LTD Data limitation

    return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]


def process_date_columns(df, date_col='DATE', date_format=None):
    """Standardize date column processing"""
    df[date_col] = pd.to_datetime(df[date_col], format=date_format)
    return filter_date(df, date_col)


def rename_and_drop(df, rename_map, drop_cols):
    """Handle column renaming and dropping"""
    return df.rename(
        columns=rename_map
    ).drop(
        columns=drop_cols, errors='ignore'
    )


def pivot_data(df, index_col, columns_col, values_col):
    """Standard pivot table creation"""
    return df.pivot_table(
        index=index_col,
        columns=columns_col,
        values=values_col,
        aggfunc='first'
    ).reset_index()


def add_weekend_column(df):
    """
    Add 'Is Weekend' bool column to DataFrame.
    """
    # 5=Saturday, 6=Sunday
    df['Is Weekend'] = df['DATE'].dt.dayofweek.isin([5, 6])

    return df


def add_holiday_column(df):
    """
    Add 'Is Holiday' bool column to DataFrame.
    """
    years = df['DATE'].dt.year.unique()
    us_holidays = holidays.US(years=years)
    holiday_dates = set(us_holidays.keys())
    df['Is Holiday'] = df['DATE'].dt.date.isin(holiday_dates)

    return df
