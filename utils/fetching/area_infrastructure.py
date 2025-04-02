import pandas as pd
import geopandas as gpd


class AreaRailwayInfrastructureService:
    ENCODING: str = 'utf-8'
    CRS: str = 'EPSG:4326'
    JOIN_TYPE: str = 'left'
    SJOIN_PREDICATE_TYPE: str = 'predicate'
    SELECTED_COLS: list[str] = ['geometry', 'JPT_NAZWA_', 'JPT_KOD_JE']
    ID_VOIVODESHIP_COL: str = 'id_wojewodztwo'
    ID_COUNTY_COL: str = 'id_powiat'
    ID_DISTRICT_COL: str = 'id_gmina'
    NAME_VOIVODESHIP_COL: str = 'nazwa_wojewodztwo'
    NAME_COUNTY_COL: str = 'nazwa_powiat'
    NAME_DISTRICT_COL: str = 'nazwa_gmina'
    GEOMETRY_COLNAME: str = 'geometry'
    STATION_COLNAME: str = 'stacja'
    WITHIN_NEW_COLNAME: str = 'in_poland'
    INDEX_RIGHT_COLNAME: str = 'index_right'
    BORDERS_SPATIAL_DATA_DIR: str = 'data/external_data/borders'

    def __init__(self, districts_filename: str, counties_filename: str, voivodeships_filename: str, country_borders_filename: str):
        self.districts_path: str = f'{self.BORDERS_SPATIAL_DATA_DIR}/districts/{districts_filename}.shp'
        self.counties_path: str = f'{self.BORDERS_SPATIAL_DATA_DIR}/counties/{counties_filename}.shp'
        self.voivodeships_path: str = f'{self.BORDERS_SPATIAL_DATA_DIR}/voivodeships/{voivodeships_filename}.shp'
        self.country_borders_path: str = f'{self.BORDERS_SPATIAL_DATA_DIR}/country/{country_borders_filename}.shp'
        self.delays_data_df: pd.DataFrame = None
        self.stations_gdf: gpd.GeoDataFrame = None
        self.country_borders_gdf: gpd.GeoDataFrame = None
        self.voivodeships_borders_gdf: gpd.GeoDataFrame = None
        self.counties_borders_gdf: gpd.GeoDataFrame = None
        self.districts_borders_gdf: gpd.GeoDataFrame = None

    def prepare_joined_station_area_data(self, stations_gdf: gpd.GeoDataFrame, delays_data_df: pd.DataFrame) -> gpd.GeoDataFrame:
        self.delays_data_df = delays_data_df
        self.stations_gdf = stations_gdf
        self._load_spatial_layers()
        stations_gps_df = self._prepare_stations_gps()
        stations_gps_df = self._flag_stations_in_poland(stations_gps_df)
        enriched_stations = self._spatial_join_areas(stations_gps_df)
        return enriched_stations, self.districts_borders_gdf, self.counties_borders_gdf

    def _load_spatial_layers(self):
        self.poland_borders = gpd.read_file(self.country_borders_gdf, encoding = self.ENCODING).to_crs(self.CRS)
        self.wojewodztwa = gpd.read_file(self.voivodeships_borders_gdf, encoding = self.ENCODING).to_crs(self.CRS)
        self.powiaty = gpd.read_file(self.counties_borders_gdf, encoding = self.ENCODING).to_crs(self.CRS)
        self.gminy = gpd.read_file(self.districts_borders_gdf, encoding = self.ENCODING).to_crs(self.CRS)

    def _prepare_stations_gps(self) -> gpd.GeoDataFrame:
        merged = pd.merge(self.delays_data_df, self.stations_gdf, on=self.STATION_COLNAME, how=self.JOIN_TYPE)
        stations_gps_df = merged[[self.STATION_COLNAME, self.GEOMETRY_COLNAME]].drop_duplicates().reset_index(drop=True)
        return gpd.GeoDataFrame(stations_gps_df, geometry=self.GEOMETRY_COLNAME, crs=self.CRS)

    def _flag_stations_in_poland(self, stations_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        stations_df[self.WITHIN_NEW_COLNAME] = stations_df.within(self.poland_borders.unary_union)
        return stations_df

    def _spatial_join_areas(self, stations_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        join_layers = [
            (self.wojewodztwa[self.SELECTED_COLS], self.ID_VOIVODESHIP_COL, self.NAME_VOIVODESHIP_COL),
            (self.powiaty[self.SELECTED_COLS], self.ID_COUNTY_COL, self.NAME_COUNTY_COL),
            (self.gminy[self.SELECTED_COLS], self.ID_DISTRICT_COL, self.NAME_DISTRICT_COL),
        ]

        enriched = stations_df.copy()
        for layer, id_col, name_col in join_layers:
            enriched = gpd.sjoin(
                enriched,
                layer,
                how = self.JOIN_TYPE,
                predicate = self.SJOIN_PREDICATE_TYPE
            ).drop(columns=[self.INDEX_RIGHT_COLNAME])
            enriched.rename(columns={self.SELECTED_COLS[-1]: id_col, self.SELECTED_COLS[1]: name_col}, inplace=True)

        return enriched
    
def measure_linestring_distance_inside_polygon(routes_list: list, polygon):
    intersection_lin_str = []
    for route in routes_list:
        inter = route.intersection(polygon)
        intersection_lin_str.append(inter)
        
    route_gdf = gpd.GeoDataFrame(geometry=intersection_lin_str)
    route_gdf.crs = "EPSG:4326"
    route_gdf_projected = route_gdf.to_crs("EPSG:32610")
    total_distance_km = route_gdf_projected.geometry.length.sum() / 1000
    return total_distance_km