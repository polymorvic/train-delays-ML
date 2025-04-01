import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Polygon


class AreaRailwayInfrastructureService:

    def __init__(self, gminy_path: str, powiaty_path: str):
        self.gminy_path = gminy_path
        self.powiaty_path = powiaty_path

        self.gminy_gdf = gpd.read_file(self.gminy_path)
        self.powiaty_gdf = gpd.read_file(self.powiaty_path)

    def get_area_codes(self) -> tuple[pd.Series, pd.Series]:
        gminy_codes = self.gminy_gdf['JPT_KOD_JE'].unique()
        powiaty_codes = self.powiaty_gdf['JPT_KOD_JE'].unique()
        return gminy_codes, powiaty_codes

    def get_area_polygon(self, area_type: str, code: str) -> Polygon:
        if area_type == 'gmina':
            area_gdf = self.gminy_gdf
        elif area_type == 'powiat':
            area_gdf = self.powiaty_gdf
        else:
            raise ValueError(f"Unsupported area type: {area_type}")

        return area_gdf.loc[area_gdf['JPT_KOD_JE'] == code, 'geometry'].item()

    def get_area_dataframe(self, area_type: str) -> gpd.GeoDataFrame:
        if area_type == 'gmina':
            return self.gminy_gdf
        elif area_type == 'powiat':
            return self.powiaty_gdf
        else:
            raise ValueError(f"Unsupported area type: {area_type}")