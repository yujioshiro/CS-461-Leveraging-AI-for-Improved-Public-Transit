# AirNow AQI Data Scraper

## Overview
This script scrapes hourly AQI data from AirNow's public file repository (http://files.airnowtech.org/). The current implementation:

- Accepts command-line arguments for specifying an **input directory** (for raw .dat files) and an **output directory** (for UTF-8 converted files).
- Ensures that the specified directories exist before proceeding.
- Generates URLs for each day's data in a specified year range.
- Fetches and verifies the availability of .dat file listings for each day.

## Usage
Run the script with the required arguments:
```bash
python main.py --input-dir <path_to_input_directory> --output-dir <path_to_output_directory>
```

### Example:
```bash
python main.py --input-dir ./data/raw --output-dir ./data/processed
```
This will:
- Ensure that `./data/raw/` and `./data/processed/` exist.
- Scrape the list of hourly AQI data files from AirNow for the default year range (2014 only in the current version).
- Print successful retrievals and failures.

## Current Progress
- ✅ Argument parsing for input/output directories
- ✅ Directory creation if missing
- ✅ Generation of daily data URLs for a given year
- ✅ Fetching and verifying page responses for .dat file availability
- ❌ Extraction of actual hourly .dat file links (next step)
- ❌ Parallel file downloading
- ❌ Encoding conversion from ISO-8859-1 to UTF-8

## Next Steps
- Implement logic to extract hourly `.dat` file links from each day's page.
- Download multiple `.dat` files in parallel using threading.
- Convert files from `ISO-8859-1` to `UTF-8` before saving.
- Add error handling and retry mechanisms for failed requests.
