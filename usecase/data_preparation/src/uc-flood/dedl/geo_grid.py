import numpy as np
from numpy._typing import NDArray
from numpy.random.mtrand import Sequence

ERA5L_RESOLUTION_DEG = 0.1
EARTH_RADIUS = 6371e3


def haversine_distance(lat_a, lon_a, lat_b, lon_b):
    lat1_rad, lon1_rad = np.radians(lat_a), np.radians(lon_a)
    lat2_rad, lon2_rad = np.radians(lat_b), np.radians(lon_b)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    distance = EARTH_RADIUS * c
    return distance


def calc_grid_box_area(lats: Sequence[float], lons: Sequence[float]) -> NDArray:
    lat_matrix, lon_matrix = np.meshgrid(lats, lons, indexing='ij')

    lat1, lon1 = lat_matrix - ERA5L_RESOLUTION_DEG / 2, lon_matrix - ERA5L_RESOLUTION_DEG / 2
    lat2, lon2 = lat_matrix + ERA5L_RESOLUTION_DEG / 2, lon_matrix - ERA5L_RESOLUTION_DEG / 2
    lat3, lon3 = lat_matrix + ERA5L_RESOLUTION_DEG / 2, lon_matrix + ERA5L_RESOLUTION_DEG / 2
    lat4, lon4 = lat_matrix - ERA5L_RESOLUTION_DEG / 2, lon_matrix + ERA5L_RESOLUTION_DEG / 2

    side1 = haversine_distance(lat1, lon1, lat2, lon2)
    side2 = haversine_distance(lat2, lon2, lat3, lon3)
    side3 = haversine_distance(lat3, lon3, lat4, lon4)
    side4 = haversine_distance(lat4, lon4, lat1, lon1)
    diag1 = haversine_distance(lat1, lon1, lat3, lon3)
    diag2 = haversine_distance(lat2, lon2, lat4, lon4)

    # Compute the area using Bretschneider's formula
    s = (side1 + side2 + side3 + side4) / 2
    return np.sqrt((s - side1) * (s - side2) * (s - side3) * (s - side4) - (diag1 * diag2) ** 2 / 16)
