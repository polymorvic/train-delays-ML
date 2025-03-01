import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from typing import Callable
from .raw_data import TrainDelaysRawDataHandler
from ..fetching.geocoding import GoogleMapsGeocoder
from ..fetching.weather import WeatherDataFetcher

FetchingMethodSelector = dict[str, Callable]

class SaveMethodSelector:
    FILE_ENCODING: str = 'utf-8'
    CRS: str = 'EPSG:4326'
    SOURCE_GEOM_COLNAMES: list[str] = ['lat', 'lon',]
    DEST_GEOM_COLNAME: str = 'geometry'

    def __init__(self, data_type: str):
        self.save_method_caller: dict[str, list[Callable[[pd.DataFrame, str, str], None]]] = {
            'csv': [self.__save_data_csv],
            'parquet': [self.__save_data_parquet],
            'shp': [self.__save_data_shp],
            'shp_csv': [self.__save_data_shp, self.__save_data_csv],
            'shp_parquet': [self.__save_data_shp, self.__save_data_parquet],
        }

        self.data_type: str = data_type.lower()
        if self.data_type not in ('stations', 'weather',):
            raise ValueError(f'Only stations ans weather data types are supported.')

    def __save_data_csv(self, input_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / f'{filename}.csv'
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        input_data.to_csv(fullpath, index = False, encoding = self.FILE_ENCODING)
        print(f"Saved in CSV format: {fullpath}")

    def __save_data_parquet(self, input_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / f'{filename}.parquet'
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        input_data.to_parquet(fullpath, index = False, engine='pyarrow')
        print("Saving in Parquet format")

    def __save_data_shp(self, input_data: pd.DataFrame, filepath:str, filename: str):
        fullpath = Path(filepath) / self.data_type / f"{filename}.shp"
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        gdf = self.__convert_df_to_geodf(input_data)
        gdf.to_file(fullpath, index=False, encoding = self.FILE_ENCODING)
        print("Saving in SHP format")

    def __convert_df_to_geodf(self, input_df: pd.DataFrame) -> gpd.GeoDataFrame:
        if not all(col in input_df.columns for col in self.SOURCE_GEOM_COLNAMES):
            raise ValueError(f"Missing required columns {self.SOURCE_GEOM_COLNAMES} in DataFrame.")
        df = input_df.copy()
        df[self.DEST_GEOM_COLNAME] = df.apply(lambda row: Point(row[self.SOURCE_GEOM_COLNAMES[1]], row[self.SOURCE_GEOM_COLNAMES[0]]), axis=1)
        return gpd.GeoDataFrame(df, geometry = self.DEST_GEOM_COLNAME, crs = self.CRS).drop(self.SOURCE_GEOM_COLNAMES, axis=1)

    def save(self, format_type: str, input_data: pd.DataFrame, filepath: str, filename: str):
        methods = self.save_method_caller.get(format_type)
        if not methods:
            raise ValueError(f"Unsupported format '{format_type}'. Available options: {list(self.save_method_caller.keys())}")
        
        for method in methods:
            method(input_data, filepath, filename)

class DataComposer(TrainDelaysRawDataHandler):
    STATION_COLNAME: str = 'stacja'
    LAT_COLNAME: str = 'lat'
    LON_COLNAME: str = 'lon'
    DATE_COLNAME: str = 'data'
    JOIN_TYPE: str = 'left'
    DEBUG: bool = True

    def __init__(self, filename, out_filename, 
                 station_df_out_filename: str,
                 weather_df_out_filename: str, 
                 autosave = True, 
                 geocoding_method: str = 'google') -> None:
        super().__init__(filename, out_filename, autosave)

        self.__geocoding_method: str = geocoding_method.lower()
        if self.__geocoding_method not in ('google', 'osm'):
            raise ValueError(f'Only google and osm method are supported, not {self.__geocoding_method}')

        self.station_df_out_filename = station_df_out_filename
        self.weather_df_out_filename = weather_df_out_filename
        self.station_names: list[str] = None
        self.stations_df: pd.DataFrame = None
        self.weather_data_input_df: pd.DataFrame = None
        self.weather_df: pd.DataFrame = None
        self.gm_geocoding_service = GoogleMapsGeocoder()
        self.weather_data_service = WeatherDataFetcher()
        self.save_method_selector = None

        self.fetching_stations_data_method_caller: FetchingMethodSelector = {
            'google': self.gm_geocoding_service.batch_geocode,
            'osm': 'placeholder',
        }

    def run_composing(self, stations_data_save_format: str, weather_data_save_format: str):
        self.__compose_stations_data(stations_data_save_format)
        self.__compose_weather_data(weather_data_save_format)

    def __compose_stations_data(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('stations')
        self.station_names: list[str] = self.get_main_data()[self.STATION_COLNAME].unique()

        if self.DEBUG:
            self.stations_df = pd.DataFrame(self.fetching_stations_data_method_caller[self.__geocoding_method](self.station_names[:2]))
            print(self.stations_df)
        else:
            self.stations_df = pd.DataFrame(self.fetching_stations_data_method_caller[self.__geocoding_method](self.station_names))

        if self.autosave:
            self.save_method_selector.save(save_format, self.stations_df, self.PREPROCESSED_DATA_DIR, self.station_df_out_filename)

    def __compose_weather_data(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('weather')
        self.__prepare_input_for_weather_data_fetching()

        if self.DEBUG:
            self.weather_df = self.weather_data_service.batch_fetch_weather(self.weather_data_input_df.iloc[:2])
            print(self.weather_df)
        else:
            self.weather_df = self.weather_data_service.batch_fetch_weather(self.weather_data_input_df.iloc)

        if self.autosave:
            self.save_method_selector.save(save_format, self.stations_df, self.PREPROCESSED_DATA_DIR, self.weather_df_out_filename)
    
    def __prepare_input_for_weather_data_fetching(self) -> None:
        self.weather_data_input_df: pd.DataFrame = self.get_main_data()[[self.STATION_COLNAME, self.DATE_COLNAME]].drop_duplicates()
        self.weather_data_input_df = self.weather_data_input_df.merge(self.stations_df, how=self.JOIN_TYPE, on=self.STATION_COLNAME)
        self.weather_data_input_df[self.DATE_COLNAME] = pd.to_datetime(self.weather_data_input_df[self.DATE_COLNAME], format='%d.%m.%Y')

    def get_main_data(self) -> pd.DataFrame:
        return super().get_merged_data()