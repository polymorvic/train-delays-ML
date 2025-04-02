import pandas as pd
import geopandas as gpd
import osmnx as ox
from pathlib import Path
from shapely.geometry import Point, LineString
from typing import Callable
from .raw_data import TrainDelaysRawDataHandler
from ..fetching.geocoding import GoogleMapsGeocoder
from ..fetching.weather import WeatherDataFetcher
from ..fetching.routes import GoogleMapsRouteFetcher
from ..fetching.railway_infrastructure import RailwayDataService, convert_to_geometry, create_small_polygon
from ..fetching.area_infrastructure import AreaRailwayInfrastructureService, measure_linestring_distance_inside_polygon

FetchingMethodSelector = dict[str, Callable]

class SaveMethodSelector:
    FILE_ENCODING: str = 'utf-8'
    CRS: str = 'EPSG:4326'
    SOURCE_GEOM_COLNAMES: list[str] = ['lat', 'lon',]
    SOURCE_SINGLE_GEOM_COLNAME: str = 'decoded_polyline'
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
        if self.data_type not in ('stations', 'weather', 'routes',):
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
        df = input_df.copy()
        geom_transformations = {
            tuple(self.SOURCE_GEOM_COLNAMES): lambda row: Point(row[self.SOURCE_GEOM_COLNAMES[1]], row[self.SOURCE_GEOM_COLNAMES[0]]),
            (self.SOURCE_SINGLE_GEOM_COLNAME,): lambda row: LineString(row[self.SOURCE_SINGLE_GEOM_COLNAME])
        }
        key = next((k for k in geom_transformations if set(k).issubset(df.columns)), None)
        if not key:
            raise ValueError(f"Missing required geometry columns: {self.SOURCE_GEOM_COLNAMES} or {self.SOURCE_SINGLE_GEOM_COLNAME} in DataFrame.")
        
        df[self.DEST_GEOM_COLNAME] = df.apply(geom_transformations[key], axis=1)
        return gpd.GeoDataFrame(df, geometry=self.DEST_GEOM_COLNAME, crs=self.CRS).drop(columns=list(key))

    def save(self, format_type: str, input_data: pd.DataFrame, filepath: str, filename: str):
        methods = self.save_method_caller.get(format_type)
        if not methods:
            raise ValueError(f"Unsupported format '{format_type}'. Available options: {list(self.save_method_caller.keys())}")
        
        for method in methods:
            method(input_data, filepath, filename)

class DataComposer(TrainDelaysRawDataHandler):
    STATION_COLNAME: str = 'stacja'
    RELATION_COLNAME: str = 'relacja'
    GEOMETRY_COLNAME: str = 'geometry'
    DECODED_POLYLINES_COLNAME: str = 'decoded_polylines'
    ID_COLNAME: str = 'id'
    KEY_COLNAME: str = 'key'
    DISTRICT_ID_COLNAME: str = 'id_gmina'
    DISTRICT_PREFIX_COLNAME: str = 'gmina'
    COUNTY_ID_COLNAME: str = 'id_powiat'
    COUNTY_PREFIX_COLNAME: str = 'powiat'
    JPT_CODE_COLNAME: str = 'JPT_KOD_JE'
    TRANSFORM_HELPER_COLNAME: str = 'full_route_station_count'
    TRANSFORM_AGG_METHOD: str = 'count'
    LAT_COLNAME: str = 'lat'
    LON_COLNAME: str = 'lon'
    DATE_COLNAME: str = 'data'
    JOIN_TYPE: str = 'left'
    DISTRICTS_FILENAME: str = 'gminy'
    COUNTIES_FILENAME: str = 'powiaty'
    VOIVODESHIPS_FILENAME: str = 'wojewodztwa'
    COUNTRY_BORDERS_FILENAME: str = 'A00_Granice_panstwa'
    BORDERS_SPATIAL_DATA_DIR: str = 'data/external_data/borders'
    COUNTRY_BORDERS_SPATIAL_FILEPATH: str = f'{BORDERS_SPATIAL_DATA_DIR}/country/{COUNTRY_BORDERS_FILENAME}.shp'
    BUFFER_SIZE: float = 0.00001
    DEBUG: bool = True

    def __init__(self, filename, out_filename, 
                 station_df_out_filename: str,
                 weather_df_out_filename: str, 
                 routes_df_out_filename: str, 
                 autosave = True, 
                 geocoding_method: str = 'google') -> None:
        super().__init__(filename, out_filename, autosave)

        self.__geocoding_method: str = geocoding_method.lower()
        if self.__geocoding_method not in ('google', 'osm'):
            raise ValueError(f'Only google and osm method are supported, not {self.__geocoding_method}')

        self.station_df_out_filename = station_df_out_filename
        self.weather_df_out_filename = weather_df_out_filename
        self.routes_df_out_filename = routes_df_out_filename
        self.station_names: list[str] = None
        self.stations_df: pd.DataFrame = None
        self.weather_data_input_df: pd.DataFrame = None
        self.routes_data_input_df: pd.DataFrame = None
        self.weather_df: pd.DataFrame = None
        self.routes_df: pd.DataFrame = None
        self.level_crossings_gs: gpd.GeoSeries = None
        self.switches_gs: gpd.GeoSeries = None
        self.districts_df: pd.DataFrame = None
        self.counties_df: pd.DataFrame = None
        self.area_infrastructure_gdf: gpd.GeoDataFrame = None
        self.gm_geocoding_service = GoogleMapsGeocoder()
        self.weather_data_service = WeatherDataFetcher()
        self.gm_routes_service = GoogleMapsRouteFetcher()
        self.railway_data_service = RailwayDataService(self.COUNTRY_BORDERS_SPATIAL_FILEPATH)
        self.area_infrastructure_service = AreaRailwayInfrastructureService(self.DISTRICTS_FILENAME, self.COUNTIES_FILENAME, self.VOIVODESHIPS_FILENAME, self.COUNTRY_BORDERS_FILENAME)
        self.save_method_selector = None

        self.fetching_stations_data_method_caller: FetchingMethodSelector = {
            'google': self.gm_geocoding_service.batch_geocode,
            'osm': 'placeholder',
        }

    def run_composing(self, stations_data_save_format: str, 
                      weather_data_save_format: str, 
                      routes_data_save_format: str, 
                      railway_infrastructure_data_save_format: str,
                      area_infrastructure_data_save_format: str):
        self.__compose_stations_data(stations_data_save_format)
        self.__compose_weather_data(weather_data_save_format)
        self.__compose_routes_data(routes_data_save_format)
        self.__compose_railway_infrastructure_data(railway_infrastructure_data_save_format)
        self.__compose_area_infrastructure(area_infrastructure_data_save_format)

    def __compose_stations_data(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('stations')
        self.station_names: list[str] = self.get_main_data()[self.STATION_COLNAME].unique()

        if self.DEBUG:
            self.stations_df = pd.DataFrame(self.fetching_stations_data_method_caller[self.__geocoding_method](self.station_names[:2]))
            # print(self.stations_df)
        else:
            self.stations_df = pd.DataFrame(self.fetching_stations_data_method_caller[self.__geocoding_method](self.station_names))

        if self.autosave:
            self.save_method_selector.save(save_format, self.stations_df, self.PREPROCESSED_DATA_DIR, self.station_df_out_filename)

    def __compose_weather_data(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('weather')
        self.__prepare_input_for_weather_data_fetching()

        if self.DEBUG:
            self.weather_df = self.weather_data_service.batch_fetch_weather(self.weather_data_input_df.iloc[:2])
            # print(self.weather_df)
        else:
            self.weather_df = self.weather_data_service.batch_fetch_weather(self.weather_data_input_df.iloc)

        if self.autosave:
            self.save_method_selector.save(save_format, self.weather_df, self.PREPROCESSED_DATA_DIR, self.weather_df_out_filename)

    def __compose_routes_data(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('routes')
        self.__prepare_input_for_routes_data_fetching()

        if self.DEBUG:
            self.routes_data_input_df = self.routes_data_input_df.iloc[:2]
            
        grouped_df = self.routes_data_input_df.groupby(self.KEY_COLNAME)
        for _, df in grouped_df:
            temp_df = df.copy().reset_index(drop=True)

            max_indice: int = temp_df.index.max() + 1
            for i in range(1, max_indice):
                current_station = temp_df.iloc[i]
                previous_station = temp_df.iloc[i - 1]
                start_lat, start_lon, dest_lat, dest_lon = previous_station[self.LAT_COLNAME], previous_station[self.LON_COLNAME], current_station[self.LAT_COLNAME], current_station[self.LON_COLNAME]
                self.gm_routes_service.get_route(start_lat, start_lon, dest_lat, dest_lon)

        self.routes_df = self.gm_routes_service.get_routes_data()

        if self.autosave:
            self.save_method_selector.save(save_format, self.routes_df, self.PREPROCESSED_DATA_DIR, self.routes_df_out_filename)

    def __compose_railway_infrastructure_data(self, save_format: str): 
        self.level_crossings_gs = self.railway_data_service.get_level_crossings()
        self.switches_gs = self.railway_data_service.get_switches()

        self.__modify_railway_infrastructure_data(self.BUFFER_SIZE)

        self.routes_df[['level_crossing_count', 'switches_count']] = self.routes_df[self.GEOMETRY_COLNAME].apply(
            lambda route: pd.Series(self.__count_railway_infrastructure_intersections(route))
            )

        if self.autosave:
            self.save_method_selector = SaveMethodSelector('railway_level_crossings')
            self.save_method_selector.save(save_format, self.level_crossings_gs.to_frame(name='geometry'), self.PREPROCESSED_DATA_DIR, 'routes_data_infrastructure')

    def __compose_area_infrastructure(self, save_format: str):
        self.save_method_selector = SaveMethodSelector('area_railway_infrastructure')
        self.area_infrastructure_gdf, self.districts_df, self.counties_df = self.area_infrastructure_service.prepare_joined_station_area_data(self.stations_df, self.get_main_data())

        districts_railway_data = self.__fetch_area_railway_data(
            area_codes=self.area_infrastructure_gdf[self.DISTRICT_ID_COLNAME].dropna().unique(),
            area_df=self.districts_df,
            area_code_col=self.JPT_CODE_COLNAME,
            area_id_col=self.DISTRICT_ID_COLNAME,
            prefix=self.DISTRICT_PREFIX_COLNAME
            )

        counties_railway_data = self.__fetch_area_railway_data(
            area_codes=self.area_infrastructure_gdf[self.COUNTY_ID_COLNAME].dropna().unique(),
            area_df=self.counties_df,
            area_code_col=self.JPT_CODE_COLNAME,
            area_id_col=self.COUNTY_ID_COLNAME,
            prefix=self.COUNTY_PREFIX_COLNAME
        )

        joined_df = (
            self.area_infrastructure_gdf
            .merge(districts_railway_data, how = self.JOIN_TYPE, on=self.DISTRICT_ID_COLNAME)
            .merge(counties_railway_data, how = self.JOIN_TYPE, on=self.COUNTY_ID_COLNAME)
        )

        if self.autosave:
            self.save_method_selector.save(save_format, joined_df, self.PREPROCESSED_DATA_DIR, 'area_railway_infrastructure')

    def __prepare_input_for_weather_data_fetching(self) -> None:
        self.weather_data_input_df: pd.DataFrame = self.get_main_data()[[self.STATION_COLNAME, self.DATE_COLNAME]].drop_duplicates()
        self.weather_data_input_df = self.weather_data_input_df.merge(self.stations_df, how=self.JOIN_TYPE, on=self.STATION_COLNAME)
        self.weather_data_input_df[self.DATE_COLNAME] = pd.to_datetime(self.weather_data_input_df[self.DATE_COLNAME], format='%d.%m.%Y')

    def __prepare_input_for_routes_data_fetching(self) -> None:
        train_delays_data = self.get_main_data()
        train_delays_data[self.TRANSFORM_HELPER_COLNAME] = train_delays_data.groupby([self.ID_COLNAME, self.RELATION_COLNAME])[self.RELATION_COLNAME].transform(self.TRANSFORM_AGG_METHOD)
        train_delays_data = train_delays_data.drop_duplicates(subset=[self.ID_COLNAME, self.RELATION_COLNAME, self.STATION_COLNAME])
        train_delays_data = train_delays_data.groupby([self.RELATION_COLNAME, self.TRANSFORM_HELPER_COLNAME])[self.STATION_COLNAME].agg(self._unique_list_preserve_order).reset_index()
        train_delays_data[self.KEY_COLNAME] = train_delays_data[self.RELATION_COLNAME].astype(str) + '_' + train_delays_data[self.STATION_COLNAME].astype(str)
        train_delays_data = train_delays_data.explode(self.STATION_COLNAME)[[self.KEY_COLNAME, self.RELATION_COLNAME, self.STATION_COLNAME]].drop_duplicates().reset_index(drop=True)

        if self.DEBUG:
            loaded_stations_df = pd.read_parquet('data/temp/ok_data/stations.parquet')
            self.routes_data_input_df = train_delays_data.merge(loaded_stations_df, how = self.JOIN_TYPE, on = self.STATION_COLNAME)
        else:
            self.routes_data_input_df = train_delays_data.merge(self.stations_df, how = self.JOIN_TYPE, on = self.STATION_COLNAME)

    def __modify_railway_infrastructure_data(self, buffer_size: float) -> None:
        self.routes_df[self.GEOMETRY_COLNAME] = self.routes_df[self.DECODED_POLYLINES_COLNAME].apply(convert_to_geometry)
        self.level_crossings_gs = create_small_polygon(self.switches_gs, buffer_size)
        self.switches_gs = create_small_polygon(self.switches_gs, buffer_size)

    def __count_railway_infrastructure_intersections(self, route):

        if isinstance(route, Point):
            return -1, -1

        if isinstance(route, LineString):
            if not hasattr(self, '_lvl_crossing_sindex'):
                self._lvl_crossing_sindex = self.level_crossings_gs.sindex
            if not hasattr(self, '_switches_sindex'):
                self._switches_sindex = self.switches_gs.sindex

            possible_lvl_crossing_idx = list(self._lvl_crossing_sindex.query(route.bounds))
            possible_lvl_crossing = self.level_crossings_gs.iloc[possible_lvl_crossing_idx]
            lvl_crossing_count = possible_lvl_crossing.apply(route.intersects).sum()

            possible_switches_idx = list(self._switches_sindex.query(route.bounds))
            possible_switches = self.switches_gs.iloc[possible_switches_idx]
            switches_count = possible_switches.apply(route.intersects).sum()

            return lvl_crossing_count, switches_count

        return -1, -1
    
    def __fetch_area_railway_data(self, area_codes, area_df, area_code_col, area_id_col, prefix):
        results = {
            area_id_col: [],
            f'railway_distance_{prefix}': [],
            f'stations_odometer_{prefix}': [],
            f'level_crossing_odometer_{prefix}': [],
            f'switches_odometer_{prefix}': [],
        }

        for code in area_codes:
            try:
                polygon = area_df.loc[area_df[area_code_col] == code, 'geometry'].item()
                railway_features = ox.geometries_from_polygon(polygon, tags={'railway': True}).reset_index()

                routes = railway_features[
                    (railway_features['element_type'] == 'way')
                    & (railway_features['railway'] == 'rail')
                    & (~railway_features['ref'].isna())
                ]['geometry'].tolist()

                switches = railway_features[
                    (railway_features['element_type'] == 'node')
                    & (railway_features['railway'] == 'switch')
                    & (~railway_features['ref'].isna())
                ]['geometry'].tolist()

                level_crossings = railway_features[
                    (railway_features['element_type'] == 'node')
                    & (railway_features['railway'] == 'level_crossing')
                    & (~railway_features['ref'].isna())
                ]['geometry'].tolist()

                stations = railway_features[
                    (railway_features['element_type'] == 'node')
                    & (railway_features['railway'].isin(['station', 'halt']))
                ]['geometry'].tolist()

                total_distance = measure_linestring_distance_inside_polygon(routes, polygon)

                results[area_id_col].append(code)
                results[f'railway_distance_{prefix}'].append(total_distance)
                results[f'stations_odometer_{prefix}'].append(len(stations))
                results[f'level_crossing_odometer_{prefix}'].append(len(level_crossings))
                results[f'switches_odometer_{prefix}'].append(len(switches))

            except (KeyError, ValueError) as e:
                print(f'[{prefix}] {code} - {e}')
                results[area_id_col].append(code)
                results[f'railway_distance_{prefix}'].append(-1)
                results[f'stations_odometer_{prefix}'].append(-1)
                results[f'level_crossing_odometer_{prefix}'].append(-1)
                results[f'switches_odometer_{prefix}'].append(-1)

        return pd.DataFrame(results)
        
    @staticmethod
    def _unique_list_preserve_order(input_array: list) -> list:
        return list(dict.fromkeys(input_array))

    def get_main_data(self) -> pd.DataFrame:
        return super().get_merged_data()