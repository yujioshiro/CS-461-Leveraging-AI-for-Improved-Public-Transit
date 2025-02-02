import argparse
import os


def parse_arguments():
    """Parse command-line arguments for input and output directories."""
    parser = argparse.ArgumentParser(description="AirNow AQI Data Scraper")
    parser.add_argument(
        "--input-dir", required=True,
        help="Directory to store downloaded .dat files"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to store UTF-8 converted files"
    )

    return parser.parse_args()


def ensure_directories_exist(input_dir, output_dir):
    """
    Ensure that the input and output directories exist.
    Creates them if necessary.
    """
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
