from typing import Optional
from pydantic import BaseModel

class RoutePolyline(BaseModel):
    encodedPolyline: Optional[str] = None

class Route(BaseModel):
    distanceMeters: Optional[int] = None
    duration: Optional[str] = None
    polyline: Optional[RoutePolyline] = None

class RouteResponse(BaseModel):
    routes: Optional[list[Route]] = []