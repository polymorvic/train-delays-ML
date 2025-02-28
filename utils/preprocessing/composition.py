import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from typing import Callable
from .raw_data import TrainDelaysRawDataHandler
from ..fetching.geocoding import GoogleMapsGeocoder

FetchingMethodSelector = dict[str, Callable]

class SaveMethodSelector:
    FILE_ENCODING: str = 'utf-8'
    CRS: str = 'EPSG:4326'
    SPATIAL_DATA_SUBDIR_NAME: str = 'stations'
    SOURCE_GEOM_COLNAMES: list[str] = ['lat', 'lon',]
    DEST_GEOM_COLNAME: str = 'geometry'

    def __init__(self):
        self.save_method_caller: dict[str, list[Callable[[pd.DataFrame, str, str], None]]] = {
            'csv': [self.__save_stations_data_csv],
            'parquet': [self.__save_stations_data_parquet],
            'shp': [self.__save_stations_data_shp],
            'shp_csv': [self.__save_stations_data_shp, self.__save_stations_data_csv],
            'shp_parquet': [self.__save_stations_data_shp, self.__save_stations_data_parquet],
        }

    def __save_stations_data_csv(self, stations_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / f'{filename}.csv'
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        stations_data.to_csv(fullpath, index = False, encoding = self.FILE_ENCODING)
        print(f"Saved in CSV format: {fullpath}")

    def __save_stations_data_parquet(self, stations_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / f'{filename}.parquet'
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        stations_data.to_parquet(fullpath, index = False, engine='pyarrow')
        print("Saving in Parquet format")

    def __save_stations_data_shp(self, stations_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / self.SPATIAL_DATA_SUBDIR_NAME / f"{filename}.shp"
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        gdf = self.__convert_df_to_geodf(stations_data)
        gdf.to_file(fullpath, index=False, encoding = self.FILE_ENCODING)
        print("Saving in SHP format")

    def __convert_df_to_geodf(self, input_df: pd.DataFrame) -> gpd.GeoDataFrame:
        if not all(col in input_df.columns for col in self.SOURCE_GEOM_COLNAMES):
            raise ValueError(f"Missing required columns {self.SOURCE_GEOM_COLNAMES} in DataFrame.")
        df = input_df.copy()
        df[self.DEST_GEOM_COLNAME] = df.apply(lambda row: Point(row[self.SOURCE_GEOM_COLNAMES[1]], row[self.SOURCE_GEOM_COLNAMES[0]]), axis=1)
        return gpd.GeoDataFrame(df, geometry = self.DEST_GEOM_COLNAME, crs = self.CRS).drop(self.SOURCE_GEOM_COLNAMES, axis=1)

    def save(self, format_type: str, stations_data: pd.DataFrame, filepath: str, filename: str):
        methods = self.save_method_caller.get(format_type)
        if not methods:
            raise ValueError(f"Unsupported format '{format_type}'. Available options: {list(self.save_method_caller.keys())}")
        
        for method in methods:
            method(stations_data, filepath, filename)

class DataComposer(TrainDelaysRawDataHandler):
    STATION_COLNAME = 'stacja'

    def __init__(self, filename, out_filename, 
                 station_df_out_filename: str, 
                 autosave = True, 
                 geocoding_method: str = 'google') -> None:
        super().__init__(filename, out_filename, autosave)

        self.__geocoding_method: str = geocoding_method.lower()
        if self.__geocoding_method not in ('google', 'osm'):
            raise ValueError(f'Only google and osm method are supported, not {self.__geocoding_method}')

        self.station_df_out_filename = station_df_out_filename
        self.station_names: list[str] = None
        self.stations_df: pd.DataFrame = None
        self.gm_geocoding_service = GoogleMapsGeocoder()
        self.save_method_selector = SaveMethodSelector()

        self.fetching_stations_data_method_caller: FetchingMethodSelector = {
            'google': self.gm_geocoding_service.batch_geocode,
            'osm': 'placeholder',
        }

    def run_composing(self, stations_data_save_format: str):
        self.__compose_stations_data(stations_data_save_format)

    def __compose_stations_data(self, save_format: str):
        self.station_names: list[str] = self.get_main_data()[self.STATION_COLNAME].unique()
        self.stations_df = pd.DataFrame(self.fetching_stations_data_method_caller[self.__geocoding_method](self.station_names))

        if self.autosave:
            self.save_method_selector.save(save_format, self.stations_df, self.PREPROCESSED_DATA_DIR, self.station_df_out_filename)

    def get_main_data(self) -> pd.DataFrame:
        return super().get_merged_data()