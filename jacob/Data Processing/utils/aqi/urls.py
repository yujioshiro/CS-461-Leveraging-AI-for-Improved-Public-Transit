from datetime import datetime, timedelta


def generate_day_urls(year, start_month=1, end_month=12):
    base_url = "https://files.airnowtech.org/?prefix=airnow/"
    day_urls = []

    for month in range(start_month, end_month + 1):
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)

        for day in range(1, last_day.day + 1):
            day_str = f"{year}{month:02d}{day:02d}"
            day_urls.append(base_url + f"airnow/{year}/{day_str}/")

    return day_urls


def generate_hourly_file_urls(
    start_year=2014,
    end_year=2025,
    start_month=1,  # New parameter
    end_month=12    # New parameter
):
    base_url = (
        "https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/"
    )
    file_urls = []

    for year in range(start_year, end_year + 1):
        # Determine month range for this year
        year_start_month = start_month if year == start_year else 1
        year_end_month = end_month if year == end_year else 12

        day_urls = generate_day_urls(year, year_start_month, year_end_month)

        for day_url in day_urls:
            date_part = day_url.strip('/').split('/')[-1]
            for hour in range(24):
                filename = f"HourlyData_{date_part}{hour:02d}.dat"
                full_url = base_url + f"{year}/{date_part}/{filename}"
                file_urls.append(full_url)

    return file_urls
