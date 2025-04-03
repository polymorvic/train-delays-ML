from pathlib import Path
from typing import Callable
import pandas as pd


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


        
