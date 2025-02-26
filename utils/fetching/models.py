from pydantic import BaseModel
from typing import Optional

class AddressComponent(BaseModel):
    long_name: str
    short_name: str
    types: list[str]

class Location(BaseModel):
    lat: float
    lng: float

class Viewport(BaseModel):
    northeast: Location
    southwest: Location

class Geometry(BaseModel):
    location: Location
    location_type: str
    viewport: Viewport

class NavigationPoint(BaseModel):
    location: dict[str, float]

class PlusCode(BaseModel):
    compound_code: str
    global_code: str

class Result(BaseModel):
    address_components: list[AddressComponent]
    formatted_address: str
    geometry: Geometry
    navigation_points: Optional[list[NavigationPoint]] = None
    place_id: str
    plus_code: Optional[PlusCode] = None
    types: list[str]

class GMGeolocator(BaseModel):
    results: list[Result]
    status: str
