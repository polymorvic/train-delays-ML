import geopandas as gpd
import osmnx as ox
from abc import ABC, abstractmethod
from shapely.geometry import Point, LineString

class OSMDataLoader(ABC):
    """Abstract base class to load railway data from OSM using a polygon."""

    def __init__(self, polygon_path: str):
        self.polygon_path = polygon_path
        self.polygon = None
        self.railway_features = None
        self._configure_osmnx()

    def _configure_osmnx(self):
        """Prevent OSMNX cache folder creation."""
        ox.settings.use_cache = False

    def load_polygon(self):
        """Load polygon geometry from shapefile."""
        self.polygon = gpd.read_file(self.polygon_path).geometry.item()

    @abstractmethod
    def load_data(self):
        """Abstract method to load OSM data."""
        pass


class RailwayFilterMixin:
    """Mixin to filter specific railway infrastructure."""

    @staticmethod
    def filter_level_crossings(features):
        """Return GeoSeries of level crossings with valid 'ref'."""
        return features[
            (features["railway"] == "level_crossing") & (~features["ref"].isna())
        ]["geometry"].reset_index(drop=True)

    @staticmethod
    def filter_switches(features):
        """Return GeoSeries of switches with valid 'ref'."""
        return features[
            (features["railway"] == "switch") & (~features["ref"].isna())
        ]["geometry"].reset_index(drop=True)


class RailwayDataService(OSMDataLoader, RailwayFilterMixin):
    """Service to load and filter railway infrastructure data."""

    def load_data(self):
        """Load railway data from OSM."""
        if self.polygon is None:
            self.load_polygon()
        tags = {"railway": True}
        self.railway_features = ox.geometries_from_polygon(self.polygon, tags).reset_index()

    def get_level_crossings(self):
        """Retrieve level crossings as GeoSeries."""
        if self.railway_features is None:
            self.load_data()
        return self.filter_level_crossings(self.railway_features)

    def get_switches(self):
        """Retrieve switches as GeoSeries."""
        if self.railway_features is None:
            self.load_data()
        return self.filter_switches(self.railway_features)
    
def convert_to_geometry(row):
    coords = row["decoded_polylines"]
    if not coords:
        return None 
    return Point(coords[0]) if len(coords) == 1 else LineString(coords)

def create_small_polygon(points_series: gpd.GeoSeries, distance: float) -> gpd.GeoSeries:
    return points_series.apply(lambda point: point.buffer(distance))

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
