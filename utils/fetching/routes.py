import requests, os
import pandas as pd
from polyline import decode
from haversine import haversine
from typing import Optional
from .models.google_maps_routes_response import Route, RouteResponse

class GoogleMapsRouteFetcher(requests.Session):
    BASE_URL: str = "https://routes.googleapis.com/directions/v2:computeRoutes"
    HEADERS: dict[str, str] = {
        "Content-Type": "application/json",
        "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline"
    }
    DATA_DICT_KEYS: list = ['start_lat', 'start_lon', 'dest_lat', 'dest_lon', 'distance_m', 'duration', 'encoded_polyline_str', 'decoded_polyline',]
    
    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            raise ValueError("Google Maps API Key is missing. Set it in the environment variables.")
        
        self.__data_dict: dict = {key: [] for key in self.DATA_DICT_KEYS}

    def __fetch_route(self, start_lat: float, start_lon: float, dest_lat: float, dest_lon: float) -> Optional[Route]:
        req_body = {
            "origin": {"location": {"latLng": {"latitude": start_lat, "longitude": start_lon}}},
            "destination": {"location": {"latLng": {"latitude": dest_lat, "longitude": dest_lon}}},
            "travelMode": "TRANSIT",
            "transitPreferences": {"allowedTravelModes": ["TRAIN", "RAIL"]}
        }

        headers = self.HEADERS.copy()
        headers["X-Goog-Api-Key"] = self.api_key

        response = self.post(self.BASE_URL, json=req_body, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            return None

        try:
            data = RouteResponse(**response.json())
            if not data.routes:
                print("No routes found. Check input coordinates.")
                return None
            return data.routes[0]
        except Exception as e:
            print(f"Parsing error: {e}")
            return None
        
    def get_route(self, start_lat: float, start_lon: float, dest_lat: float, dest_lon: float) -> None:
        route = self.__fetch_route(start_lat, start_lon, dest_lat, dest_lon)
        if route:
            original_route_distance_m: float = route.distanceMeters
            original_route_duration_s: str = route.duration
            original_route_encoded_polyline_str: str = route.polyline.encodedPolyline

            route_duration_s, decoded_polyline = self._handle_original_response_data(original_route_duration_s, original_route_encoded_polyline_str)
            values = [start_lat, start_lon, dest_lat, dest_lon, original_route_distance_m, route_duration_s, original_route_encoded_polyline_str, decoded_polyline]

        else:
            values = [None] * len(self.DATA_DICT_KEYS)

        for key, value in zip(self.DATA_DICT_KEYS, values):
            self.__data_dict[key].append(value)

    def get_routes_data(self) -> pd.DataFrame:
        return pd.DataFrame(self.__data_dict)

    @staticmethod
    def _polyline_decoder(encoded_polyline_str: str) -> list[tuple[float, float]]:
        return decode(encoded_polyline_str)
    
    @staticmethod
    def _handle_original_response_data(duration: str, polyline: str) -> tuple[float, list[tuple[float, float]]]:
        route_duration_s: float  = float(duration.replace('s', ''))
        decoded_polyline: list[tuple[float, float]] = GoogleMapsRouteFetcher._polyline_decoder(polyline) 
        return route_duration_s, decoded_polyline