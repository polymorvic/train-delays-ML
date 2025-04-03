from pathlib import Path
from typing import Callable
from datetime import datetime
import pandas as pd
import numpy as np
from haversine import haversine, Unit
from scipy.spatial import cKDTree
from .const import MAIN_RAILWAY_STATIONS, MODELING_READY_COLNAMES_BLACKLIST


class LoadMethodSelector:
    FILE_ENCODING: str = 'utf-8'

    def __init__(self):
        self.load_method_caller: dict[str, Callable[..., pd.DataFrame]] = {
            'csv': self.__load_data_csv,
            'parquet': self.__load_data_parquet,
            'excel': self.__load_data_excel,
        }

    def __load_data_csv(self, filepath: str, **kwargs) -> pd.DataFrame:
        return pd.read_csv(filepath, encoding=self.FILE_ENCODING, **kwargs)

    def __load_data_parquet(self, filepath: str, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(filepath, **kwargs)

    def __load_data_excel(self, filepath: str, **kwargs) -> pd.DataFrame:
        return pd.read_excel(filepath, **kwargs)

    def load(self, filepath: str, **kwargs) -> pd.DataFrame:
        ext = Path(filepath).suffix.lower().lstrip('.')
        method = self.load_method_caller.get(ext)
        if not method:
            raise ValueError(f"Unsupported file extension '.{ext}'. Available options: {list(self.load_method_caller.keys())}")
        return method(filepath, **kwargs)


class DataAssembler:
    PREPROCESSED_DATA_DIR = Path('data/preprocessed')
    GUS_EXTERNAL_DATA_DIR = Path('data/external/gus')
    OUTPUT_DATA_DIR = Path('data/ready_for_modeling')
    OUTPUT_DIR = Path('data/ready_for_modeling')
    FILE_ENCODING = 'utf-8'
    TIMESTAMP = datetime.now().strftime('%Y%m%d_%H_%M_%S')

    def __init__(self, main_delays_data_filename: str,
                 stations_data_filename: str,
                 routes_data_filename: str,
                 weather_data_filename: str,
                 area_railway_infrastructure_data_filename: str,
                 gus_district_data_filename: str,
                 gus_counties_data_filename: str,
                 autosave: bool = True) -> None:
        
        self.loader = LoadMethodSelector()
        self.autosave = autosave
        self.filenames = {
            'main_delays': (main_delays_data_filename, self.PREPROCESSED_DATA_DIR),
            'stations': (stations_data_filename, self.PREPROCESSED_DATA_DIR),
            'routes': (routes_data_filename, self.PREPROCESSED_DATA_DIR),
            'weather': (weather_data_filename, self.PREPROCESSED_DATA_DIR),
            'area_railway': (area_railway_infrastructure_data_filename, self.PREPROCESSED_DATA_DIR),
            'gus_district': (gus_district_data_filename, self.GUS_EXTERNAL_DATA_DIR),
            'gus_counties': (gus_counties_data_filename, self.GUS_EXTERNAL_DATA_DIR),
            }
        self.file_read_options = {
            'gus_district': {'sep': ';'},
            'gus_counties': {'sep': ';'},
        }
        self.dataframes = self.__load_data()
        self.prepared_for_modeling_delays_df = None
        
    def __load_data(self) -> dict[str, pd.DataFrame]:
        loaded = {}
        for key, (fname, directory) in self.filenames.items():
            file_path = directory / fname
            try:
                kwargs = self.file_read_options.get(key, {})
                df = self.loader.load(file_path, **kwargs)
                loaded[key] = df
            except Exception as e:
                raise RuntimeError(f"[DataAssembler] Failed to load '{file_path}': {e}") from e
        return loaded
    
    def run_assembling(self) -> None:
        self.__prepare_raw_data_stations_merge()
        self.__count_stations()
        self.__merge_routes_data()
        self.__calculate_distances()
        self.__merge_weather_data()
        self.__add_stop_duration_features()
        self.__apply_date_features()
        self.__merge_area_railway_infrastructure()
        self.__merge_gus_data()

        if self.autosave:
            self.prepared_for_modeling_delays_df[MODELING_READY_COLNAMES_BLACKLIST].drop_duplicates().reset_index(drop=True).to_parquet(f'{self.OUTPUT_DATA_DIR}/ready_to_modeling_data_{self.TIMESTAMP}.parquet')

    def __prepare_raw_data_stations_merge(self) -> None:
        main_delays_df = self.dataframes['main_delays']
        stations_df = self.dataframes['stations']

        self.prepared_delays_df = self.prepare_raw_data(main_delays_df)
        self.prepared_delays_df = pd.merge(
            self.prepared_delays_df,
            stations_df,
            on='stacja',
            how='left'
        )

    def __count_stations(self) -> None:
        data = self.prepared_delays_df
        data['station_count_on_curr_station'] = data.groupby(['id', 'relacja']).cumcount()
        data['full_route_station_count'] = data.groupby(['id', 'relacja'])['relacja'].transform('count')

    def __merge_routes_data(self) -> None:
        data = self.prepared_delays_df.copy().reset_index(drop=True)
        routes_data = self.dataframes['routes']

        route_keys = self.__create_route_keys(data)
        data = data.merge(route_keys[['id', 'relacja', 'key']], on=['id', 'relacja'], how='left')
        data = self.__add_prev_next_stations(data)
        merged_df = self.__filter_and_merge_routes(data, routes_data)

        self.prepared_for_modeling_delays_df = merged_df

    def __calculate_distances(self) -> None:
        df_out = self.prepared_for_modeling_delays_df.copy()
        df_out = self.__add_cumulative_distance_features(df_out)
        df_out = self.__add_nearest_big_city_distance(df_out)
        self.prepared_for_modeling_delays_df = df_out

    def __merge_weather_data(self) -> None:
        weather = self.dataframes['weather'].copy()

        weather.drop(columns=['stations', 'source', 'tzoffset', 'datetimeEpoch'], inplace=True, errors='ignore')
        weather['datetime_merge'] = pd.to_datetime(weather['date'].astype(str) + ' ' + weather['datetime'].astype(str))

        df_out = self.prepared_for_modeling_delays_df.copy()
        df_out['datetime_merge'] = df_out['arrival_on_time'].dt.floor('h')

        merged_df = pd.merge(
            df_out,
            weather,
            how='left',
            on=['lat', 'lon', 'datetime_merge']
        ).drop(columns=['date', 'datetime_merge', 'datetime'], errors='ignore')

        self.prepared_for_modeling_delays_df = merged_df

    def __add_stop_duration_features(self) -> None:
        df = self.prepared_for_modeling_delays_df.copy()
        df['stop_duration'] = (df['departure_on_time'] - df['arrival_on_time']).dt.total_seconds() / 60

        lag_features = {}
        for i in range(1, 7):
            lag_col = f'stop_duration_lag{i}'
            lag_features[lag_col] = (
                df.groupby(['id', 'relacja'])['stop_duration']
                .shift(i)
                .fillna(-1)
            )
        df = pd.concat([df, pd.DataFrame(lag_features)], axis=1)
        self.prepared_for_modeling_delays_df = df

    def __apply_date_features(self) -> None:
        df = self.prepared_for_modeling_delays_df.copy()

        for col in ['arrival_on_time', 'departure_on_time']:
            df = self.fix_dates(df, col)

        df = self.add_date_features(df)

        self.prepared_for_modeling_delays_df = df

    def __merge_area_railway_infrastructure(self) -> None:
        administrative_units = self.dataframes['area_railway']
        df = self.prepared_for_modeling_delays_df.copy()

        df = pd.merge(
            df,
            administrative_units,
            how='left',
            on=['stacja', 'lat', 'lon']
        )

        df[['id_gmina', 'id_powiat']] = (
            df[['id_gmina', 'id_powiat']].fillna(-1).astype(int)
        )

        self.prepared_for_modeling_delays_df = df

    def __merge_gus_data(self) -> None:
        df_out = self.prepared_for_modeling_delays_df.copy()
        gus_districts = self.dataframes['gus_district'][[
            'id_gmina', 'powierzchnia_km2_gmina', 'ludnosc_gmina', 'gestosc_zaludnienia_1km2_gmina'
        ]]
        gus_counties = self.dataframes['gus_counties'][[
            'id_powiat', 'powierzchnia_km2_powiat', 'ludnosc_powiat', 'gestosc_zaludnienia_1km2_powiat'
        ]]

        df_out = df_out.merge(gus_districts, how='left', on='id_gmina')
        df_out = df_out.merge(gus_counties, how='left', on='id_powiat')

        self.prepared_for_modeling_delays_df = df_out

    def __create_route_keys(self, data: pd.DataFrame) -> pd.DataFrame:
        route_keys = (
            data.groupby(['id', 'relacja'])['stacja']
            .agg(self.unique_list_preserve_order)
            .reset_index()
        )
        route_keys['key1'] = route_keys['relacja']
        route_keys['key2'] = route_keys['stacja'].apply(lambda x: ', '.join(x))
        route_keys['key'] = route_keys['key1'] + '_' + route_keys['key2']
        return route_keys

    def __add_prev_next_stations(self, data: pd.DataFrame) -> pd.DataFrame:
        data['prev_stations'] = data.groupby(['id', 'relacja'])['stacja'].shift(1)
        data['next_stations'] = data.groupby(['id', 'relacja'])['stacja'].shift(-1)
        return data

    def __filter_and_merge_routes(self, data: pd.DataFrame, routes_data: pd.DataFrame) -> pd.DataFrame:
        merged_df = data.merge(
            routes_data,
            how='left',
            on=['key', 'relacja', 'stacja', 'lat', 'lon', 'prev_stations', 'next_stations']
        )
        keys_to_remove = merged_df[merged_df['distances'].isna()]['key'].unique()
        merged_df = merged_df[~merged_df['key'].isin(keys_to_remove)].reset_index(drop=True)
        return merged_df
    
    def __add_cumulative_distance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df['cumsum_distances'] = df.groupby('id')['distances'].cumsum()
        df['distance_to_finish'] = df.groupby('id')['cumsum_distances'].transform(lambda x: x.max() - x)
        df[['distances', 'cumsum_distances', 'distance_to_finish']] /= 1000
        return df

    def __add_nearest_big_city_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        big_city_stations = self.__get_big_city_stations()
        station_coords = big_city_stations[['lat', 'lon']].to_numpy()
        kdtree = cKDTree(station_coords)

        query_coords = df[['lat', 'lon']].to_numpy()
        _, indices = kdtree.query(query_coords, k=1)

        distances = self.__calculate_haversine_distances(df, big_city_stations, indices)
        df['nearest_big_city_distance'] = distances
        return df

    def __get_big_city_stations(self) -> pd.DataFrame:
        stations_df = self.dataframes['stations']
        return stations_df[stations_df['stacja'].isin(MAIN_RAILWAY_STATIONS)].reset_index(drop=True)
    
    @staticmethod
    def prepare_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df['przyjazd planowy'] = np.where(df['przyjazd planowy'].isnull(), df['odjazd planowy'], df['przyjazd planowy'])
        df['odjazd planowy'] = np.where(df['odjazd planowy'].isnull(), df['przyjazd planowy'], df['odjazd planowy'])

        df['data'] = pd.to_datetime(df['data'], format='%d.%m.%Y')
        df['przyjazd planowy'] = pd.to_datetime(df['przyjazd planowy'], format='%H:%M:%S', errors='coerce').dt.time
        df['odjazd planowy'] = pd.to_datetime(df['odjazd planowy'], format='%H:%M:%S', errors='coerce').dt.time

        df['arrival_on_time'] = pd.to_datetime(
            df['data'].dt.strftime('%Y-%m-%d') + ' ' + df['przyjazd planowy'].astype(str),
            errors='coerce'
        )
        df['departure_on_time'] = pd.to_datetime(
            df['data'].dt.strftime('%Y-%m-%d') + ' ' + df['odjazd planowy'].astype(str),
            errors='coerce'
        )

        delay_cols = ["opóźnienie przyjazdu", "opóźnienie odjazdu"]
        for col in delay_cols:
            df[col] = (
                df[col].str.replace("min", "", regex=False)
                    .str.replace("---", "0", regex=False)
                    .str.strip()
                    .astype(float)
            )

        df['numer pociągu'] = df['numer pociągu'].str.split().str[0]

        train_type_mapping = {
            'ECE': 'EuroCity',
            'EIE': 'Intercity', 'EIJ': 'Intercity',
            'ENE': 'EuroNight',
            'MHE': 'InterVoivodeshipExpressHotel', 'MHS': 'InterVoivodeshipExpressHotel',
            'MME': 'InternationalExpress', 'MMM': 'InternationalExpress',
            'MOE': 'InterVoivodeshipStopping', 'MOJ': 'InterVoivodeshipStopping', 'MOM': 'InterVoivodeshipStopping',
            'MPE': 'InterVoivodeshipExpress', 'MPJ': 'InterVoivodeshipExpress', 'MPM': 'InterVoivodeshipExpress', 'MPS': 'InterVoivodeshipExpress',
            'RAJ': 'LocalAglomeration', 'RAM': 'LocalAglomeration',
            'RMJ': 'LocalInternational', 'RMM': 'LocalInternational',
            'ROE': 'LocalDomestic', 'ROJ': 'LocalDomestic', 'ROM': 'LocalDomestic', 'ROS': 'LocalDomestic',
            'RPE': 'LocalDomesticExpress', 'RPJ': 'LocalDomesticExpress', 'RPM': 'LocalDomesticExpress', 'RPS': 'LocalDomesticExpress'
        }

        traction_type_electric = {
            'ECE', 'EIE', 'EIJ', 'ENE', 'MHE', 'MME', 'MOE', 'MOJ',
            'MPE', 'MPJ', 'RAJ', 'RMJ', 'ROE', 'ROJ', 'RPE'
        }

        df['train_type'] = df['numer pociągu'].map(train_type_mapping)

        df['traction_type'] = np.where(
            df['numer pociągu'].isin(traction_type_electric),
            'electric',
            'combustion'
        )
        df.drop(['data', 'przyjazd planowy', 'odjazd planowy', 'numer pociągu'], axis=1, inplace=True)

        return df
    
    @staticmethod
    def unique_list_preserve_order(input_list: list):
        seen = set()
        unique_items = []
        for item in input_list:
            if item not in seen:
                unique_items.append(item)
                seen.add(item)
        return unique_items
    
    @staticmethod
    def __calculate_haversine_distances(df: pd.DataFrame, big_city_stations: pd.DataFrame, indices: np.ndarray) -> list[float]:
        return [
            haversine(
                (row.lat, row.lon),
                (big_city_stations.iloc[idx]['lat'], big_city_stations.iloc[idx]['lon']),
                unit=Unit.KILOMETERS
            )
            for row, idx in zip(df.itertuples(index=False), indices)
        ]
    
    @staticmethod
    def fix_dates(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        np_day = np.timedelta64(1, 'D')
        dfs = []

        for pk, df_group in df.groupby('id'):
            dates = df_group[col_name].values
            diff = (dates[1:] - dates[:-1]) / np_day
            change_indices = np.where(diff < 0)[0]

            if change_indices.size:
                change_on = change_indices[0] + 1
                df_group.loc[df_group.index[change_on]:, col_name] += pd.Timedelta(days=1)

            dfs.append(df_group)

        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def add_date_features(df: pd.DataFrame) -> pd.DataFrame:
        dt_col = 'arrival_on_time'

        df['month'] = df[dt_col].dt.month
        df['weekofyear'] = df[dt_col].dt.isocalendar().week
        df['yearday'] = df[dt_col].dt.dayofyear
        df['monthday'] = df[dt_col].dt.day
        df['weekday'] = df[dt_col].dt.dayofweek
        df['hour'] = df[dt_col].dt.hour
        df['minute'] = df[dt_col].dt.minute
        df['second'] = df[dt_col].dt.second

        cyclical = {
            'month': 12, 'weekofyear': 52, 'yearday': 365, 
            'monthday': 31, 'weekday': 7, 'hour': 24, 'minute': 60, 'second': 60
        }

        for col, period in cyclical.items():
            df[f'{col}_sin'] = np.sin(2 * np.pi * df[col] / period)
            df[f'{col}_cos'] = np.cos(2 * np.pi * df[col] / period)

        df.drop(list(cyclical.keys()), axis=1, inplace=True)

        def days_until(row, month, day):
            current_year = row[dt_col].year
            holiday = pd.Timestamp(year=current_year, month=month, day=day)
            return (holiday - row[dt_col]).days

        df['days_until_christmas'] = df.apply(lambda row: days_until(row, 12, 25), axis=1)
        df['days_until_november_1_st'] = df.apply(lambda row: days_until(row, 11, 1), axis=1)
        df['days_until_new_year_eve'] = df.apply(lambda row: days_until(row, 12, 31), axis=1)
        df['days_until_easter'] = df.apply(lambda row: days_until(row, 4, 5), axis=1)

        return df
