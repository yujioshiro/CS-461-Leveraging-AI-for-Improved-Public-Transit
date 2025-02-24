import holidays


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
