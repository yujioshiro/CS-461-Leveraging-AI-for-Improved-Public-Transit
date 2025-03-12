import os
import pandas as pd
from ..general.processing import process_date_columns
from ..general.io import read_combine_csvs, save_data
from ..general.processing import rename_and_drop


pd.set_option('future.no_silent_downcasting', True)


class WeatherProcessor:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

        # Boolean weather conversion columns
        self.boolean_columns = [
            'fog_ice_freezing_fog', 'heavy_fog', 'thunder',
            'ice_pellets_sleet', 'hail', 'glaze_rime', 'dust_or_sand',
            'smoke_haze', 'blowing_snow', 'tornado_funnel_cloud',
            'high_damaging_winds', 'blowing_spray', 'mist', 'drizzle',
            'freezing_drizzle', 'rain', 'freezing_rain', 'snow',
            'unknown_precipitation', 'ground_fog', 'ice_fog'
        ]

        # Comprehensive fill rules
        self.fill_rules = {
            # Boolean columns
            **{col: False for col in self.boolean_columns},

            # Numerical columns
            'precipitation': 0.0,
            'snowfall': 0.0,
            'snow_depth': 0.0,
            'max_temperature': 0.0,
            'min_temperature': 0.0,
            'temperature_at_observation': 0.0,
            'average_wind_speed': 0.0,
            'fastest_2min_wind_speed': 0.0,
            'fastest_5sec_wind_speed': 0.0,
            'fastest_2min_wind_direction': 0.0,
            'fastest_5sec_wind_direction': 0.0,
            'days_in_multiday_precipitation_total': 0.0,
            'multiday_precipitation_total': 0.0,
            'peak_gust_time': 0.0,
            'water_equivalent_snow_ground': 0.0,
            'water_equivalent_snowfall': 0.0,
            'average_temperature': 0.0,
        }

        self.rename_map = {
            'WT01': 'fog_ice_freezing_fog',
            'WT02': 'heavy_fog',
            'WT03': 'thunder',
            'WT04': 'ice_pellets_sleet',
            'WT05': 'hail',
            'WT06': 'glaze_rime',
            'WT07': 'dust_or_sand',
            'WT08': 'smoke_haze',
            'WT09': 'blowing_snow',
            'WT10': 'tornado_funnel_cloud',
            'WT11': 'high_damaging_winds',
            'WT12': 'blowing_spray',
            'WT13': 'mist',
            'WT14': 'drizzle',
            'WT15': 'freezing_drizzle',
            'WT16': 'rain',
            'WT17': 'freezing_rain',
            'WT18': 'snow',
            'WT19': 'unknown_precipitation',
            'WT21': 'ground_fog',
            'WT22': 'ice_fog',
            'PRCP': 'precipitation',
            'SNOW': 'snowfall',
            'SNWD': 'snow_depth',
            'TMAX': 'max_temperature',
            'TMIN': 'min_temperature',
            'TOBS': 'temperature_at_observation',
            'AWND': 'average_wind_speed',
            'WSF2': 'fastest_2min_wind_speed',
            'WSF5': 'fastest_5sec_wind_speed',
            'WDF2': 'fastest_2min_wind_direction',
            'WDF5': 'fastest_5sec_wind_direction',
            'DAPR': 'days_in_multiday_precipitation_total',
            'MDPR': 'multiday_precipitation_total',
            'PGTM': 'peak_gust_time',
            'WESD': 'water_equivalent_snow_ground',
            'WESF': 'water_equivalent_snowfall',
            'TAVG': 'average_temperature',
            'PRCP_ATTRIBUTES': 'precipitation_attributes',
            'SNOW_ATTRIBUTES': 'snowfall_attributes',
            'SNWD_ATTRIBUTES': 'snow_depth_attributes',
            'TMAX_ATTRIBUTES': 'max_temperature_attributes',
            'TMIN_ATTRIBUTES': 'min_temperature_attributes',
            'TOBS_ATTRIBUTES': 'temperature_at_observation_attributes',
            'AWND_ATTRIBUTES': 'average_wind_speed_attributes',
            'WSF2_ATTRIBUTES': 'fastest_2min_wind_speed_attributes',
            'WSF5_ATTRIBUTES': 'fastest_5sec_wind_speed_attributes',
            'WDF2_ATTRIBUTES': 'fastest_2min_wind_direction_attributes',
            'WDF5_ATTRIBUTES': 'fastest_5sec_wind_direction_attributes',
            'DAPR_ATTRIBUTES': 'days_in_multiday_precipitation_total_'
                               'attributes',
            'MDPR_ATTRIBUTES': 'multiday_precipitation_total_attributes',
            'PGTM_ATTRIBUTES': 'peak_gust_time_attributes',
            'WESD_ATTRIBUTES': 'water_equivalent_snow_ground_attributes',
            'WESF_ATTRIBUTES': 'water_equivalent_snowfall_attributes',
            'TAVG_ATTRIBUTES': 'average_temperature_attributes',
            'WT01_ATTRIBUTES': 'fog_ice_freezing_fog_attributes',
            'WT02_ATTRIBUTES': 'heavy_fog_attributes',
            'WT03_ATTRIBUTES': 'thunder_attributes',
            'WT04_ATTRIBUTES': 'ice_pellets_sleet_attributes',
            'WT05_ATTRIBUTES': 'hail_attributes',
            'WT06_ATTRIBUTES': 'glaze_rime_attributes',
            'WT08_ATTRIBUTES': 'smoke_haze_attributes'
        }

    def process(self):
        df = self._load_data()
        df = self._clean_data(df)
        self._save_data(df)
        return df

    def _load_data(self):
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(
                f"Input directory not found: {self.input_dir}"
            )
        return read_combine_csvs(self.input_dir)

    def _clean_data(self, df):
        # 1. Filter to target date range
        df = process_date_columns(df)
        # 2. Filter to most comprehensive station
        df = self._filter_comprehensive_station(df)
        # 3. Rename and drop columns
        df = self._rename_columns(df)
        # 4. Handle missing values
        df = self._fill_missing_values(df)
        # 5. Convert boolean columns
        df = self._cast_boolean_columns(df)
        return df

    def _rename_columns(self, df):
        df = rename_and_drop(
            df, self.rename_map,
            ['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION']
        )
        return df

    def _filter_comprehensive_station(self, df):
        """Select station with most complete data records"""
        if 'STATION' not in df.columns:
            raise ValueError("STATION column missing - cannot filter stations")

        # Calculate data completeness per station
        station_quality = df.groupby('STATION').count().sum(axis=1)
        best_station = station_quality.idxmax()
        print(
            f"Selected station: {best_station} with "
            f"{station_quality[best_station]} data points"
        )

        return df[df['STATION'] == best_station]

    def _fill_missing_values(self, df):
        """Fill missing values with appropriate defaults"""
        # Fill known columns
        df = df.fillna(self.fill_rules)

        # Fill remaining numeric columns with 0
        numeric_cols = df.select_dtypes(include='number').columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        return df

    def _cast_boolean_columns(self, df):
        """Convert weather flags to proper booleans"""
        for col in self.boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        return df

    def _save_data(self, df):
        """Save processed data with validation"""
        output_path = os.path.join(self.output_dir, 'processed_weather.tsv')
        save_data(df, output_path)
        print(f"Weather processing complete. Data saved to {output_path}")
