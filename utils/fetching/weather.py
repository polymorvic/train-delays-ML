import requests, os
import pandas as pd
from typing import Optional
from .models.weather_response import WeatherData, HourlyData

class WeatherDataFetcher(requests.Session):
    BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    LAT_COLNAME: str = 'lat'
    LON_COLNAME: str = 'lon'
    DATE_COLNAME: str = 'data'
    HOURLY_DATA_MODEL_ATTRS: dict[str, str] = HourlyData.model_fields.keys()

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("WEATHER_API_KEY")
        if not self.api_key:
            raise ValueError("Weather API Key is missing. Set it in the environment variables.")
        self.response_raw_data: dict = None

    def __fetch_weather(self, latitude: float, longitude: float, fetch_date: Optional[str] = None) -> Optional[WeatherData]:
        url = f"{self.BASE_URL}/{latitude},{longitude}/{fetch_date}/{fetch_date}?unitGroup=metric&key={self.api_key}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} for coordinates ({latitude}, {longitude}) on {fetch_date}")
            return None

        try:
            self.response_raw_data = WeatherData(**response.json())
            return self.__handle_response_data(latitude, longitude)
             
        except Exception as e:
            print(f"Parsing error: {e}")
            return pd.DataFrame(columns=self.HOURLY_DATA_MODEL_ATTRS)

    def __handle_response_data(self, latitude: float, longitude: float) -> pd.DataFrame:
        hourly_raw_data: list[HourlyData] = self.response_raw_data.days[0].hours
        data_dict = {
            attr: [getattr(item, attr) for item in hourly_raw_data] 
            for attr in self.HOURLY_DATA_MODEL_ATTRS
        }
        data_dict.update({
            'lat': [latitude] * len(hourly_raw_data),
            'lon': [longitude] * len(hourly_raw_data),
        })

        return pd.DataFrame(data_dict)

    def batch_fetch_weather(self, locations_df: pd.DataFrame) -> pd.DataFrame:
        results_dfs = []
        for _, row in locations_df.iterrows():
            lat, lon, date = row[self.LAT_COLNAME], row[self.LON_COLNAME], row[self.DATE_COLNAME].strftime('%Y-%m-%d')
            weather_data = self.__fetch_weather(lat, lon, date)
            results_dfs.append(weather_data)
        return pd.concat(results_dfs, ignore_index=True)
    