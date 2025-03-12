import os
import pandas as pd
from multiprocessing import Pool
from utils.general.io import save_data
from utils.general.processing import process_date_columns
from utils.aqi.downloader import download_all_files
from utils.aqi.urls import generate_hourly_file_urls
from utils.aqi.convert import convert_all_files


class AQIProcessor:
    """
    Processes AQI data from converted files to
    final analysis-ready format
    """

    COLUMNS = [
        "DATE", "AQI TIME", "AQSID", "AQI Sitename", "AQI GMT Offset",
        "AQI Parameter", "AQI Reporting Units", "AQI Measurement",
        "AQI Data Source"
    ]

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.city_filter = "Eugene - Highway 99"
        self.columns_to_drop = [
            "AQI TIME", "AQSID", "AQI Sitename", "AQI GMT Offset",
            "AQI Parameter", "AQI Reporting Units", "AQI Measurement",
            "AQI Data Source", "DAY"
        ]

    def process(self):
        """Execute full processing pipeline"""
        combined = self._combine_data()
        if not combined.empty:
            processed = self._process_data(combined)
            self._save_output(processed)

    def _combine_data(self):
        """Read and combine all AQI files"""
        try:
            file_paths = [
                os.path.join(self.input_dir, f)
                for f in os.listdir(self.input_dir)
                if f.endswith(".dat")
            ]
            with Pool() as pool:
                results = pool.map(self._process_file, file_paths)
            return pd.concat(results, ignore_index=True)
        except Exception as e:
            print(f"Error combining data: {str(e)}")
            return pd.DataFrame()

    def _process_file(self, file_path):
        """Process individual AQI file"""
        try:
            df = pd.read_csv(file_path, sep="|", names=self.COLUMNS)
            return self._filter_and_prepare(df)
        except Exception as e:
            print(f"Error processing {os.path.basename(file_path)}: {str(e)}")
            return pd.DataFrame()

    def _filter_and_prepare(self, df):
        """Filter data and prepare date columns"""
        df = df[df["AQI Sitename"] == self.city_filter].copy()
        df = process_date_columns(df)
        df["DAY"] = df["DATE"].dt.date
        return df

    def _process_data(self, df):
        """Create daily averages and final dataset"""
        daily_avg = self._calculate_daily_averages(df)
        merged = pd.merge(df, daily_avg, on="DAY", how="left")
        return self._final_cleanup(merged)

    def _calculate_daily_averages(self, df):
        """Calculate daily average AQI values"""
        return (
            df.groupby("DAY", as_index=False)
            ["AQI Measurement"].mean()
            .round(1)
            .rename(columns={"AQI Measurement": "Average AQI"})
        )

    def _final_cleanup(self, df):
        """Perform final data cleanup"""
        df = df.drop_duplicates(subset=["DATE"], keep="first")
        df = df.drop(columns=self.columns_to_drop, errors="ignore")
        return process_date_columns(df)

    def _save_output(self, df):
        """Save processed data"""
        output_path = os.path.join(self.output_dir, "processed_aqi.tsv")
        save_data(df, output_path)
        print(f"Successfully saved processed AQI data to {output_path}")


class AQIDownloader:
    """Handles downloading and conversion of raw AQI data"""

    def __init__(self, input_dir, output_dir,
                 start_year=2014, end_year=2025,
                 start_month=1, end_month=12,
                 max_threads=50, max_processes=4):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.start_year = start_year
        self.end_year = end_year
        self.start_month = start_month
        self.end_month = end_month
        self.max_threads = max_threads
        self.max_processes = max_processes

    def execute(self):
        """Run full download and conversion pipeline"""
        self._ensure_directories()
        urls = self._generate_urls()
        self._download_files(urls)
        self._convert_files()

    def _ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def _generate_urls(self):
        """Generate file URLs for specified date range"""
        return generate_hourly_file_urls(
            self.start_year, self.end_year,
            self.start_month, self.end_month
        )

    def _download_files(self, urls):
        """Download files using threaded downloader"""
        download_all_files(urls, self.input_dir, self.max_threads)

    def _convert_files(self):
        """Convert files to UTF-8 encoding"""
        convert_all_files(self.input_dir, self.output_dir, self.max_processes)
