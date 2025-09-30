from shapely.geometry import shape, Point
import json
from typing import Any

def flatten_list(list_of_lists: list[list[Any]]) -> list[Any]:
    return [item for sublist in list_of_lists for item in sublist]

def check_daily_coverage(rows, target_polygon):
    # Create points from your DataFrame
    points = [Point(lon, lat) for lon, lat in zip(rows['longitude'], rows['latitude'])]

    # Check if all points are within the polygon
    points_in_polygon = [point.within(target_polygon) for point in points]
    coverage_percentage = sum(points_in_polygon) / len(points_in_polygon) * 100
    return coverage_percentage

def get_ski_polygon(resort_name: str):
    with open('ski_areas.geojson', 'r') as f:
        ski_areas = json.load(f)
    
    ski_area = [site for site in ski_areas['features'] if site['properties']['name'] == resort_name][0]
    return shape(ski_area['geometry'])

def get_snow_season(month, year):
    """Convert month/year to snow season year"""
    if month >= 11:  # Nov, Dec
        return f"{int(year)}-{int(year)+1}"  # Snow season starts this year
    elif month <= 4:  # Jan, Feb, Mar, Apr
        return f"{int(year)-1}-{int(year)}"  # Snow season started previous year
    else:
        return None  # Not in snow season (May-Oct)
