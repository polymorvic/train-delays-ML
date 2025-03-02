import requests, os
from typing import Optional
from .models.google_maps_geocoding_response import GMGeolocator, Result

class GoogleMapsGeocoder(requests.Session):
    BASE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
    STATION_COLNAME = 'stacja'
    LAT_COLNAME = 'lat'
    LON_COLNAME = 'lon'
    ADDITIONAL_PATTERN = ', railway station'

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            raise ValueError("Google Maps API Key is missing. Set it in the environment variables.")

    def __fetch_geocode(self, location: str) -> Optional[Result]:
        params = {
            "key": self.api_key,
            "address": f"{location}"
        }
        response = self.get(self.BASE_URL, params=params)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} for {location}")
            return None

        try:
            data = GMGeolocator(**response.json())
            if data.status != "OK" or not data.results:
                print(f"Warning: No results found for {location}")
                return None
            return data.results[0]
        except Exception as e:
            print(f"Parsing error: {e}")
            return None

    def batch_geocode(self, locations: list[str]) -> list[dict[str, str|float]]:
        results = []
        for location in locations:
            result = self.__fetch_geocode(f'{location}{self.ADDITIONAL_PATTERN}')
            results.append({
                self.STATION_COLNAME: location.replace(self.ADDITIONAL_PATTERN, ''),
                self.LAT_COLNAME: result.geometry.location.lat if result else None,
                self.LON_COLNAME: result.geometry.location.lng if result else None
            })
        return results
