
from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import os
import geopandas as gpd
import pandas as pd
import plotly.express as px
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
from shapely.strtree import STRtree
import multiprocessing as mp
import numpy as np

city_data = {
    "ms": {
        "sensor_files": [
            "./tracks/sensor_data/ms_Finedust_PM2_5.geojson",
            "./tracks/sensor_data/ms_Finedust_PM10.geojson",
            "./tracks/sensor_data/ms_Finedust_PM4.geojson",
            "./tracks/sensor_data/ms_Finedust_PM1.geojson",
            "./tracks/sensor_data/ms_Overtaking_Distance.geojson",
            "./tracks/sensor_data/ms_Rel__Humidity.geojson",
            "./tracks/sensor_data/ms_Surface_Anomaly.geojson",
            "./tracks/sensor_data/ms_Temperature.geojson",
            "./tracks/sensor_data/ms_Speed.geojson",
        ],
        "osm_file": "./tracks/BI_MS.geojson",
    },
    "os": {
        "sensor_files": [
            "./tracks/sensor_data/os_Finedust_PM2_5.geojson",
            "./tracks/sensor_data/os_Finedust_PM10.geojson",
            "./tracks/sensor_data/os_Finedust_PM4.geojson",
            "./tracks/sensor_data/os_Finedust_PM1.geojson",
            "./tracks/sensor_data/os_Overtaking_Distance.geojson",
            "./tracks/sensor_data/os_Rel__Humidity.geojson",
            "./tracks/sensor_data/os_Surface_Anomaly.geojson",
            "./tracks/sensor_data/os_Temperature.geojson",
            "./tracks/sensor_data/os_Speed.geojson",
        ],
        "osm_file": "./tracks/BI_OS.geojson"
    }
}


# def snap_to_nearest_line(point, street_index):
#     """
#     Finds the nearest point on the closest street line.
#     Uses STRtree for fast nearest lookup.
#     """
#     nearest_geom = street_index.nearest(point)
#     if isinstance(nearest_geom, LineString):
#         return nearest_points(point, nearest_geom)[1]  # Nearest point on line
#     return point  # Return original if invalid

def snap_to_nearest_line(point, streets, street_index):
    # ref:https://medium.com/data-science/connecting-pois-to-a-road-network-358a81447944
    """Finds the nearest point on the closest street line using STRtree + distance refinement."""
    
    nearest_idx = street_index.nearest(point)  # This might return an index, not a geometry!
    
    # Ensure we fetch the actual geometry using the index
    if isinstance(nearest_idx, np.int64):  # If it's an index, get the geometry
        possible_nearest_geom = streets.iloc[nearest_idx].geometry
    else:
        possible_nearest_geom = nearest_idx  # If it's already a geometry, use it
    
    # Validate the geometry
    if possible_nearest_geom is None or possible_nearest_geom.is_empty:
        return point  # Return original if invalid

    # Find the truly closest street using Euclidean distance
    nearest_index = streets.geometry.distance(point).idxmin()
    nearest_geom = streets.loc[nearest_index, "geometry"]

    return nearest_points(point, nearest_geom)[1]  # Return the exact nearest point


def snap_batch(points_chunk, streets, street_index):
    """Multiprocessing helper function to snap a batch of points."""
    return [snap_to_nearest_line(point,streets, street_index) for point in points_chunk]

def process_sensor_file(sensor_file, streets, street_index):
    """
    Loads sensor points, snaps them to streets, and aggregates sensor values.
    """
    print(f"Processing {sensor_file}...")
    sensor_name = sensor_file.split("/")[-1].replace(".geojson", "") 

    # Load points
    points = gpd.read_file(sensor_file).to_crs(streets.crs)

    # Remove invalid geometries
    points = points[points.geometry.notnull() & points.geometry.apply(lambda g: isinstance(g, Point))]
    # Check if the file is "ms_Overtaking_Distance" and replace 400 with 0
    if "ms_Overtaking_Distance" in sensor_file and "value" in points.columns:
        points.loc[points["value"] == 400, "value"] = 0
    if "ms_Finedust_PM2_5" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 100, "value"] = 125
    if "ms_Finedust_PM4" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 100, "value"] = 125
    if "ms_Finedust_PM1" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 100, "value"] = 125
    if "ms_Finedust_PM10" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 100, "value"] = 125
    if "ms_Rel__Humidity" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 93, "value"] = 93
    if "ms_Speed" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 25, "value"] = 25
    if "ms_Temperature" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 25, "value"] = 25
    if "ms_Surface_Anomaly" in sensor_file and "value" in points.columns:
        points.loc[points["value"] > 1.5, "value"] = 1.5
    # breakpoint()
    # Extract date and month from timestamp
    if "timestamp" in points.columns:
        points["date"] = pd.to_datetime(points["timestamp"]).dt.date
        points["month"] = pd.to_datetime(points["timestamp"]).dt.month

        # Filter for October, November, and December
        points = points[points["month"].isin([10, 11, 12])]

    # Convert GeoSeries to list for multiprocessing
    points_list = points.geometry.tolist()

    # Set up multiprocessing
    num_workers = max(1, os.cpu_count() - 2)  # Use available CPU cores
    chunk_size = max(1, len(points_list) // num_workers)  # Split points into chunks
    chunks = [points_list[i:i + chunk_size] for i in range(0, len(points_list), chunk_size)]

    # Run multiprocessing pool
    with mp.Pool(processes=num_workers) as pool:
        snapped_results = pool.starmap(snap_batch, [(chunk, streets, street_index) for chunk in chunks])

    # Flatten snapped results
    snapped_geoms = [point for sublist in snapped_results for point in sublist]

    # Assign snapped geometries
    # print(type(snapped_geoms), snapped_geoms[:5])
    snapped_geoms = [geom if isinstance(geom, Point) else None for geom in snapped_geoms]
    points = points.drop(columns=["geometry"], errors="ignore")  # Remove old geometry if exists
    points["geometry"] = snapped_geoms  # Assign the cleaned geometries
    points = points.dropna(subset=["geometry"])  # Remove rows with invalid geometries
    if not isinstance(points, gpd.GeoDataFrame):
        points = gpd.GeoDataFrame(points, geometry="geometry", crs=streets.crs)  # Convert

    # points["geometry"] = snapped_geoms
    # points = points.set_geometry("geometry")
    # breakpoint()
    #  Save snapped points for debugging
    # points.to_file(f"./snapped/snapped_{sensor_name}.geojson", driver="GeoJSON")
    # Spatial join (buffer method for street matching)
    buffer_size = 1  # Adjust as needed
    streets["buffered_geom"] = streets.geometry.buffer(buffer_size)
    joined = gpd.sjoin(points, streets.set_geometry("buffered_geom"), predicate="intersects")

    # Aggregate sensor values per street
    sensor_name = os.path.basename(sensor_file).replace(".geojson", "")
    agg_data = joined.groupby("index_right").agg(
        **{f"list_{sensor_name}": ("value", list), f"avg_{sensor_name}": ("value", "mean")}
    ).reset_index()

    # Merge with streets
    streets = streets.merge(agg_data, left_index=True, right_on="index_right", how="left")

    # Cleanup
    streets.drop(columns=["buffered_geom", "index_right"], errors="ignore", inplace=True)

    return streets

def process_city(city):
    """
    Main function to process all sensor files for a city.
    """
    if city not in city_data:
        raise ValueError(f"City '{city}' not found in city_data dictionary!")
    
    city_info = city_data[city]
    sensor_files = city_info["sensor_files"]

    # Load streets file
    streets = gpd.read_file(city_info["osm_file"]).to_crs(epsg=32637)
    streets = streets.explode(index_parts=False)
    streets = streets[streets.geometry.type == "LineString"]
    # Ensure it has the correct geometry type
    if "geometry" not in streets.columns:
        raise ValueError("The loaded GeoJSON does not contain a 'geometry' column.")

    streets = streets[streets.geometry.notnull()]  

    # Build STRtree index for fast nearest street lookup
    street_index = STRtree(streets.geometry.values)
    # breakpoint()
    # Process each sensor file
    for sensor_file in sensor_files:
        streets = process_sensor_file(sensor_file, streets, street_index)

    # Select final columns
    keep_columns = [
        "id", "@id", "cycleway", "description:name", "lanes", "maxspeed",
        "sidewalk", "surface", "geometry"
    ]
    
    # Add aggregated sensor columns
    for sensor_file in city_data[city]["sensor_files"]:
        sensor_name = os.path.basename(sensor_file).replace(".geojson", "")
        keep_columns.extend([f"list_{sensor_name}", f"avg_{sensor_name}"])

    # Keep only relevant columns
    streets = streets[[col for col in keep_columns if col in streets.columns]]

    # breakpoint()
    base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    os.makedirs(base_path, exist_ok=True)
    # Save updated routes to a new GeoJSON file
    output_file = f"osm_streets_{city}_winter.geojson"
    output_path = os.path.join(base_path, output_file)
    # Save final output
    # output_file = f"osm_streets_{city}_winter_newmax.geojson"
    streets.to_file(output_path, driver="GeoJSON")
    print(f"Processing completed for {city}. Output saved to {output_file}")

    return JsonResponse({ "message": f"Processing completed for {city}. Output saved to {output_file}"})


# if __name__ == "__main__":
#     # Example usage
#     process_city("ms")