import requests
from datetime import datetime, timedelta


def generate_day_urls(year, start_month=1, end_month=12):
    """
    Generates URLs for each day in a given year, considering months and days.
    """
    base_url = "https://files.airnowtech.org/?prefix=airnow/"
    day_urls = []

    # Loop through each month of the year
    for month in range(start_month, end_month + 1):
        first_day = datetime(year, month, 1)
        next_month = first_day.replace(
            month=month % 12 + 1
        ) if month < 12 else first_day.replace(
            year=year + 1, month=1
        )
        last_day = next_month - timedelta(days=1)

        for day in range(1, last_day.day + 1):
            # Construct the day URL with format "airnow/[Year]/[YearMonthDay]/"
            day_str = f"{year}{str(month).zfill(2)}{str(day).zfill(2)}"
            day_urls.append(base_url + f"airnow/{year}/{day_str}/")

    return day_urls


def get_file_urls(start_year=2014, end_year=2014):
    """
    Fetches and returns all .dat file URLs for each day in each year
    """
    file_urls = []

    # Step 1: Loop through each year from start_year to end_year
    for year in range(start_year, end_year + 1):
        day_urls = generate_day_urls(year)

        # Step 2: Loop through each day URL
        for day_url in day_urls:
            response = requests.get(day_url)

            if response.status_code != 200:
                print(
                    f"Failed to fetch {day_url} "
                    f"with status code {response.status_code}"
                )
                continue

            # Debugging
            print(f"Successfully fetched {day_url}")

            # Step 3: find .dat links matching 'HourlyData_' pattern

            # Step 4: Construct full URLs for the .dat files
            # and add them to file_urls

    return file_urls
