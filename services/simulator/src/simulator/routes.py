"""
Waypoint-based Indian transportation corridors with GPS interpolation and scheduled stops.
"""

import math
from dataclasses import dataclass
from typing import List, Tuple
import numpy as np


@dataclass(frozen=True)
class Waypoint:
    name: str
    lat: float
    lon: float
    base_ambient_c: float  # Baseline climate temp in Celsius


@dataclass(frozen=True)
class Route:
    route_id: str
    origin: str
    destination: str
    waypoints: List[Waypoint]
    total_distance_km: float


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two GPS coordinates in kilometers."""
    r = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _build_route(route_id: str, origin: str, destination: str, waypoints: List[Waypoint]) -> Route:
    dist = 0.0
    for i in range(len(waypoints) - 1):
        dist += haversine_km(
            waypoints[i].lat, waypoints[i].lon,
            waypoints[i + 1].lat, waypoints[i + 1].lon
        )
    return Route(
        route_id=route_id,
        origin=origin,
        destination=destination,
        waypoints=waypoints,
        total_distance_km=round(dist, 2),
    )


INDIAN_ROUTES: List[Route] = [
    _build_route(
        "RT-IND-BOM", "Indore", "Mumbai",
        [
            Waypoint("Indore", 22.7196, 75.8577, 31.0),
            Waypoint("Dhule", 20.9042, 74.7749, 33.0),
            Waypoint("Nashik", 19.9975, 73.7898, 29.0),
            Waypoint("Thane", 19.2183, 72.9781, 32.0),
            Waypoint("Mumbai", 19.0760, 72.8777, 32.0),
        ],
    ),
    _build_route(
        "RT-DEL-JAI", "Delhi", "Jaipur",
        [
            Waypoint("Delhi", 28.7041, 77.1025, 34.0),
            Waypoint("Gurgaon", 28.4595, 77.0266, 34.0),
            Waypoint("Neemrana", 27.9892, 76.3880, 35.0),
            Waypoint("Shahpura", 27.3919, 75.9596, 36.0),
            Waypoint("Jaipur", 26.9124, 75.7873, 36.5),
        ],
    ),
    _build_route(
        "RT-PUN-BLR", "Pune", "Bengaluru",
        [
            Waypoint("Pune", 18.5204, 73.8567, 30.0),
            Waypoint("Satara", 17.6805, 73.9997, 29.5),
            Waypoint("Kolhapur", 16.7050, 74.2433, 30.0),
            Waypoint("Belagavi", 15.8497, 74.4977, 28.0),
            Waypoint("Hubballi", 15.3647, 75.1240, 31.0),
            Waypoint("Davanagere", 14.4644, 75.9218, 31.5),
            Waypoint("Tumakuru", 13.3409, 77.1010, 29.0),
            Waypoint("Bengaluru", 12.9716, 77.5946, 27.5),
        ],
    ),
    _build_route(
        "RT-MAA-HYD", "Chennai", "Hyderabad",
        [
            Waypoint("Chennai", 13.0827, 80.2707, 33.5),
            Waypoint("Nellore", 14.4426, 79.9865, 34.0),
            Waypoint("Ongole", 15.5057, 80.0499, 34.5),
            Waypoint("Vijayawada", 16.5062, 80.6480, 36.0),
            Waypoint("Suryapet", 17.1439, 79.6239, 35.0),
            Waypoint("Hyderabad", 17.3850, 78.4867, 32.5),
        ],
    ),
    _build_route(
        "RT-AMD-BOM", "Ahmedabad", "Mumbai",
        [
            Waypoint("Ahmedabad", 23.0225, 72.5714, 35.0),
            Waypoint("Vadodara", 22.3072, 73.1812, 34.5),
            Waypoint("Bharuch", 21.7051, 72.9959, 34.0),
            Waypoint("Surat", 21.1702, 72.8311, 33.0),
            Waypoint("Vapi", 20.3893, 72.9106, 32.0),
            Waypoint("Mumbai", 19.0760, 72.8777, 32.0),
        ],
    ),
    _build_route(
        "RT-CCU-BBI", "Kolkata", "Bhubaneswar",
        [
            Waypoint("Kolkata", 22.5726, 88.3639, 32.0),
            Waypoint("Kharagpur", 22.3460, 87.2320, 33.0),
            Waypoint("Balasore", 21.4934, 86.9135, 33.5),
            Waypoint("Cuttack", 20.4625, 85.8828, 33.0),
            Waypoint("Bhubaneswar", 20.2961, 85.8245, 32.5),
        ],
    ),
    _build_route(
        "RT-NAG-HYD", "Nagpur", "Hyderabad",
        [
            Waypoint("Nagpur", 21.1458, 79.0882, 36.0),
            Waypoint("Hinganghat", 20.5593, 78.8378, 36.5),
            Waypoint("Adilabad", 19.6641, 78.5320, 37.0),
            Waypoint("Nirmal", 19.0964, 78.3428, 36.0),
            Waypoint("Nizamabad", 18.6725, 78.0941, 35.0),
            Waypoint("Hyderabad", 17.3850, 78.4867, 32.5),
        ],
    ),
    _build_route(
        "RT-LKO-DEL", "Lucknow", "Delhi",
        [
            Waypoint("Lucknow", 26.8467, 80.9462, 33.0),
            Waypoint("Bareilly", 28.3670, 79.4304, 32.5),
            Waypoint("Moradabad", 28.8386, 78.7733, 33.0),
            Waypoint("Ghaziabad", 28.6692, 77.4538, 33.5),
            Waypoint("Delhi", 28.7041, 77.1025, 34.0),
        ],
    ),
    _build_route(
        "RT-BLR-COK", "Bengaluru", "Kochi",
        [
            Waypoint("Bengaluru", 12.9716, 77.5946, 27.5),
            Waypoint("Hosur", 12.7409, 77.8253, 28.5),
            Waypoint("Salem", 11.6643, 78.1460, 33.0),
            Waypoint("Coimbatore", 11.0168, 76.9558, 31.0),
            Waypoint("Palakkad", 10.7867, 76.6548, 31.5),
            Waypoint("Thrissur", 10.5276, 76.2144, 30.5),
            Waypoint("Kochi", 9.9312, 76.2673, 29.5),
        ],
    ),
    _build_route(
        "RT-JAI-AMD", "Jaipur", "Ahmedabad",
        [
            Waypoint("Jaipur", 26.9124, 75.7873, 36.5),
            Waypoint("Ajmer", 26.4499, 74.6399, 36.0),
            Waypoint("Bhilwara", 25.3407, 74.6313, 35.5),
            Waypoint("Udaipur", 24.5854, 73.7125, 33.5),
            Waypoint("Himatnagar", 23.5977, 72.9698, 35.0),
            Waypoint("Ahmedabad", 23.0225, 72.5714, 35.0),
        ],
    ),
]


class RouteInterpolator:
    """
    Interpolates GPS coordinates and baseline ambient temperature along a Route
    given normalized progress fraction [0.0, 1.0].
    """

    def __init__(self, route: Route):
        self.route = route
        self._cumulative_distances: List[float] = [0.0]
        for i in range(len(route.waypoints) - 1):
            d = haversine_km(
                route.waypoints[i].lat, route.waypoints[i].lon,
                route.waypoints[i + 1].lat, route.waypoints[i + 1].lon
            )
            self._cumulative_distances.append(self._cumulative_distances[-1] + d)

    def interpolate(self, fraction: float) -> Tuple[float, float, float]:
        """
        Returns (latitude, longitude, base_ambient_temp_c) at progress fraction in [0, 1].
        """
        fraction = max(0.0, min(1.0, fraction))
        target_dist = fraction * self.route.total_distance_km

        for i in range(len(self._cumulative_distances) - 1):
            d_start = self._cumulative_distances[i]
            d_end = self._cumulative_distances[i + 1]
            if d_start <= target_dist <= d_end:
                seg_length = d_end - d_start
                if seg_length <= 1e-6:
                    t = 0.0
                else:
                    t = (target_dist - d_start) / seg_length

                wp1 = self.route.waypoints[i]
                wp2 = self.route.waypoints[i + 1]

                lat = wp1.lat + t * (wp2.lat - wp1.lat)
                lon = wp1.lon + t * (wp2.lon - wp1.lon)
                base_temp = wp1.base_ambient_c + t * (wp2.base_ambient_c - wp1.base_ambient_c)
                return lat, lon, base_temp

        last_wp = self.route.waypoints[-1]
        return last_wp.lat, last_wp.lon, last_wp.base_ambient_c
