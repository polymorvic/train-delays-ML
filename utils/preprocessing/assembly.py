from pathlib import Path
from typing import Callable
import pandas as pd
import numpy as np


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
    OUTPUT_DIR = Path('data/ready_for_modeling')
    FILE_ENCODING = 'utf-8'

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
        self.dataframes = self.__load_data()
        self.prepared_for_modeling_delays_df = None
        
    def __load_data(self) -> tuple[pd.DataFrame, ...]:
        loaded = {}
        for key, (fname, directory) in self.filenames.items():
            file_path = directory / fname
            try:
                df = self.loader.load(file_path)
                loaded[key] = df
            except Exception as e:
                raise RuntimeError(f"[DataAssembler] Failed to load '{file_path}': {e}") from e
        return loaded
    
    def run_assembling(self) -> None:

        self.__prepare_raw_data_stations_merge()

    def __prepare_raw_data_stations_merge(self) -> None:
        main_delays_df = self.dataframes['main_delays']
        stations_df = self.dataframes['stations']

        self.prepared_delays_df = self.__prepare_raw_data(main_delays_df)
        self.prepared_delays_df = pd.merge(
            self.prepared_delays_df,
            stations_df,
            on='stacja',
            how='left'
        )

    @staticmethod
    def __prepare_raw_data(df: pd.DataFrame) -> pd.DataFrame:
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


        
