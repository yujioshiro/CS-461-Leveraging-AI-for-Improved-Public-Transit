import pandas as pd


def rename_columns(df):
    COLUMN_RENAME_MAP = {
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
        'DAPR_ATTRIBUTES': 'days_in_multiday_precipitation_total_attributes',
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
        'WT08_ATTRIBUTES': 'smoke_haze_attributes',
    }

    return df.rename(columns=COLUMN_RENAME_MAP)


def filter_station(df):
    non_null_counts = df.groupby('STATION').count().sum(axis=1)
    comprehensive_station = non_null_counts.idxmax()
    return df[df['STATION'] == comprehensive_station]


def fill_missing_values(df):
    return df.fillna({
        'fog_ice_freezing_fog': False,
        'heavy_fog': False,
        'thunder': False,
        'ice_pellets_sleet': False,
        'hail': False,
        'glaze_rime': False,
        'dust_or_sand': False,
        'smoke_haze': False,
        'blowing_snow': False,
        'tornado_funnel_cloud': False,
        'high_damaging_winds': False,
        'blowing_spray': False,
        'mist': False,
        'drizzle': False,
        'freezing_drizzle': False,
        'rain': False,
        'freezing_rain': False,
        'snow': False,
        'unknown_precipitation': False,
        'ground_fog': False,
        'ice_fog': False,
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
    })


def cast_boolean_columns(df):
    boolean_columns = [
        'fog_ice_freezing_fog', 'heavy_fog', 'thunder', 'ice_pellets_sleet',
        'hail', 'glaze_rime', 'dust_or_sand', 'smoke_haze',
        'blowing_snow', 'tornado_funnel_cloud', 'high_damaging_winds',
        'blowing_spray', 'mist', 'drizzle', 'freezing_drizzle', 'rain',
        'freezing_rain', 'snow', 'unknown_precipitation', 'ground_fog',
        'ice_fog'
    ]
    for column in boolean_columns:
        if column in df.columns:
            df[column] = df[column].astype(bool)


def date_to_datetime(df):
    df['DATE'] = pd.to_datetime(
        df['DATE'], format='%Y-%m-%d', errors='coerce'
    )
