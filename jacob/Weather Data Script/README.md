# NOAA Weather Data Processing

This program processes NOAA weather data CSV files, combines them, and outputs the processed data in TSV and Parquet formats. It also prints summary metrics of the processed data.

## Usage

To process the weather data, run the process_weather_data.py script with the input and output directories as arguments:

```sh
python process_weather_data.py <input_dir> <output_dir>
```
<input_dir>: Directory containing input CSV files.
<output_dir>: Directory for output processed files.

### Example:
```sh
python process_weather_data.py data/ processed_output/
```