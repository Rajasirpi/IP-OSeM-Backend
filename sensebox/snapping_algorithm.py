
from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, shape
from shapely.ops import nearest_points
from shapely.strtree import STRtree
# import multiprocessing as mp
import numpy as np
# import fiona
from uuid import uuid5, NAMESPACE_URL
from shapely import wkt
import json


city_data = {
    "ms": {
        "sensor_files": [
            "./tracks/sensor_data/ms_Finedust_PM2_5.geojson",
            "./tracks/sensor_data/ms_Finedust_PM10.geojson",
            "./tracks/sensor_data/ms_Finedust_PM4.geojson",
            "./tracks/sensor_data/ms_Finedust_PM1.geojson",
            "./tracks/sensor_data/ms_Overtaking_Distance.geojson",
            "./tracks/sensor_data/ms_Rel__Humidity.geojson",
            # "./tracks/sensor_data/ms_Surface_Anomaly.geojson",
            "./tracks/sensor_data/ms_Temperature.geojson",
            "./tracks/sensor_data/ms_Speed.geojson",
            "./tracks/ms_accidents.geojson"
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
            # "./tracks/sensor_data/os_Surface_Anomaly.geojson",
            "./tracks/sensor_data/os_Temperature.geojson",
            "./tracks/sensor_data/os_Speed.geojson",
            "./tracks/os_accidents.geojson",
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

def compute_point_uid(geom, tag=None, value=None):
    """Generates a stable UUIDv5 using normalized WKT + tag + value."""

    geom_str = geom.wkt if isinstance(geom, Point) else str(geom)

    tag_str = str(tag).strip() if tag is not None else ""
    try:
        value_str = f"{float(value):.4f}" if value is not None else ""
    except Exception:
        value_str = str(value).strip()

    uid_input = f"{geom_str}_{tag_str}_{value_str}"
    return str(uuid5(NAMESPACE_URL, uid_input))

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

# def process_sensor_file(sensor_file, streets, street_index):
#     """
#     Loads sensor points, snaps them to streets, and aggregates sensor values.
#     """
#     print(f"Processing {sensor_file}...")
#     sensor_name = sensor_file.split("/")[-1].replace(".geojson", "") 

#     points = None
#     file_size = os.path.getsize(sensor_file)

#     if file_size > 50 * 1024 * 1024:  # If file > 100MB, use Fiona
#         print(f"Large file detected ({file_size / (1024*1024):.2f} MB). Using Fiona to stream data...")
#         points_all = []
#         try:
#             with fiona.open(sensor_file, 'r') as src:
#                 for feature in src:
#                     geom = shape(feature["geometry"])
#                     if not isinstance(geom, Point):
#                         continue
#                     props = feature["properties"]
#                     props["geometry"] = geom
#                     points_all.append(props)
#         except Exception as e:
#             print(f"Fiona failed to read {sensor_file}: {e}")
#             return streets

#         if not points_all:
#             print(f"No valid points found in {sensor_file}")
#             return streets

#         points = gpd.GeoDataFrame(points_all, geometry="geometry", crs=streets.crs)

#     else:
#         print(f"File size is {file_size / (1024*1024):.2f} MB. Using GeoPandas to read normally.")
#         try:
#             points = gpd.read_file(sensor_file).to_crs(streets.crs)
#         except Exception as e:
#             print(f"GeoPandas failed to read {sensor_file}: {e}")
#             return streets
        
#     # Remove invalid geometries
#     points = points[points.geometry.notnull() & points.geometry.apply(lambda g: isinstance(g, Point))]
#     # Check if the file is "ms_Overtaking_Distance" and replace 400 with 0
#     if "ms_Overtaking_Distance" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] == 400, "value"] = 0
#     if "ms_Finedust_PM2_5" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] > 180, "value"] = 180
#     if "ms_Finedust_PM4" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] > 180, "value"] = 180
#     if "ms_Finedust_PM1" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] > 180, "value"] = 180
#     if "ms_Finedust_PM10" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] > 180, "value"] = 180
#     # if "ms_Rel__Humidity" in sensor_file and "value" in points.columns:
#     #     points.loc[points["value"] > 93, "value"] = 93
#     if "ms_Speed" in sensor_file and "value" in points.columns:
#         points.loc[points["value"] > 60, "value"] = 60
#     # if "ms_Temperature" in sensor_file and "value" in points.columns:
#     #     points.loc[points["value"] > 25, "value"] = 25
#     # if "ms_Surface_Anomaly" in sensor_file and "value" in points.columns:
#     #     points.loc[points["value"] > 1.5, "value"] = 1.5
#     # breakpoint()
#     # # Extract date and month from timestamp
#     # if "timestamp" in points.columns:
#     #     points["date"] = pd.to_datetime(points["timestamp"]).dt.date
#     #     points["month"] = pd.to_datetime(points["timestamp"]).dt.month

#     #     # Filter for October, November, and December
#     #     points = points[points["month"].isin([10, 11, 12])]

#     # Convert GeoSeries to list for multiprocessing
#     points_list = points.geometry.tolist()

#     # Set up multiprocessing
#     num_workers = max(1, os.cpu_count() - 2)  # Use available CPU cores
#     chunk_size = max(1, len(points_list) // num_workers)  # Split points into chunks
#     chunks = [points_list[i:i + chunk_size] for i in range(0, len(points_list), chunk_size)]

#     # Run multiprocessing pool
#     with mp.Pool(processes=num_workers) as pool:
#         snapped_results = pool.starmap(snap_batch, [(chunk, streets, street_index) for chunk in chunks])

#     # Flatten snapped results
#     snapped_geoms = [point for sublist in snapped_results for point in sublist]

#     # Assign snapped geometries
#     # print(type(snapped_geoms), snapped_geoms[:5])
#     snapped_geoms = [geom if isinstance(geom, Point) else None for geom in snapped_geoms]
#     points = points.drop(columns=["geometry"], errors="ignore")  # Remove old geometry if exists
#     points["geometry"] = snapped_geoms  # Assign the cleaned geometries
#     points = points.dropna(subset=["geometry"])  # Remove rows with invalid geometries
#     if not isinstance(points, gpd.GeoDataFrame):
#         points = gpd.GeoDataFrame(points, geometry="geometry", crs=streets.crs)  # Convert

#     # points["geometry"] = snapped_geoms
#     # points = points.set_geometry("geometry")
#     # breakpoint()
#     #  Save snapped points for debugging
#     # points.to_file(f"./snapped/snapped_{sensor_name}.geojson", driver="GeoJSON")
#     # Spatial join (buffer method for street matching)
#     buffer_size = 1  # Adjust as needed
#     streets["buffered_geom"] = streets.geometry.buffer(buffer_size)
#     joined = gpd.sjoin(points, streets.set_geometry("buffered_geom"), predicate="intersects")

#     # Aggregate sensor values per street
#     sensor_name = os.path.basename(sensor_file).replace(".geojson", "")
#     agg_data = joined.groupby("index_right").agg(
#         **{f"list_{sensor_name}": ("value", list), f"avg_{sensor_name}": ("value", "mean")}
#     ).reset_index()

#     # Merge with streets
#     streets = streets.merge(agg_data, left_index=True, right_on="index_right", how="left")

#     # Cleanup
#     streets.drop(columns=["buffered_geom", "index_right"], errors="ignore", inplace=True)

#     return streets


def process_sensor_file(city, sensor_file, streets, street_index):
    print(f"Processing {sensor_file}...")

    sensor_name = os.path.basename(sensor_file).replace(".geojson", "")
    is_accident = "accidents" in sensor_name.lower()
    cache_path = f"./tracks/cache/snapping_map_{sensor_name}.csv"
    os.makedirs("./tracks/cache", exist_ok=True)

    # === Load raw GeoJSON ===
    try:
        with open(sensor_file) as f:
            data = json.load(f)
        features = data.get("features", [])
        if not features:
            raise ValueError(f"No features in {sensor_file}")
        points = gpd.GeoDataFrame.from_features(features)
        points = points.set_crs("EPSG:4326").to_crs(streets.crs)
    except Exception as e:
        print(f"Error loading {sensor_file}: {e}")
        return streets

    # === Clean invalid geometries ===
    points = points[points.geometry.notnull() & points.geometry.apply(lambda g: isinstance(g, Point))]

     # === Special case for accidents ===
    if is_accident:
        # Map UKATEGORIE to weight
        category_to_weight = {3: 0.15, 2: 0.35, 1: 0.5}
        points["value"] = points["UKATEGORIE"].map(category_to_weight)
        points = points.dropna(subset=["value"])
        points["UKATEGORIE"] = points["UKATEGORIE"].astype(str).str.strip()
        points["value"] = points["value"].astype(float)
        # points["point_uid"] = points["point_uid"].astype(str).str.strip()
        points["point_uid"] = points.apply(
            lambda row: compute_point_uid(row.geometry, row["UKATEGORIE"], row["value"]),
            axis=1
        )
        # print(points.geometry.iloc[0].wkt)
    else:
        # Standard sensor handling
        if "ms_Overtaking_Distance" in sensor_file:
            points.loc[points["value"] == 400, "value"] = 0
        if "Finedust" in sensor_file:
            points.loc[points["value"] > 180, "value"] = 180
        if "ms_Speed" in sensor_file:
            points.loc[points["value"] > 60, "value"] = 60

        points = points.dropna(subset=["value"])
        points["value"] = points["value"].astype(float)
        if "timestamp" in points.columns:
            points["timestamp"] = points["timestamp"].astype(str).str.strip()
        # points["point_uid"] = points["point_uid"].astype(str).str.strip()
        points["point_uid"] = points.apply(
            lambda row: compute_point_uid(row.geometry, row.get("timestamp", ""), row["value"]),
            axis=1
        )
   
    # === Load cache if exists ===
    if os.path.exists(cache_path):
        cached_map = pd.read_csv(cache_path)
        # Normalize cached point_uid to prevent mismatch
        cached_map["point_uid"] = cached_map["point_uid"].astype(str).str.strip()
        cached_map["geometry"] = cached_map["geometry_wkt"].apply(wkt.loads)
        cached_map = gpd.GeoDataFrame(cached_map, geometry="geometry", crs=streets.crs)
    else:
        cached_map = gpd.GeoDataFrame(columns=["point_uid", "geometry", "index_right"], geometry="geometry", crs=streets.crs)

    # === Identify new points to snap ===
    snapped_uids = set(cached_map["point_uid"])
    new_points = points[~points["point_uid"].isin(snapped_uids)]
    print(f"New points to snap: {len(new_points)} | Cached: {len(cached_map)}")

    if not new_points.empty:
        # Snap
        snapped_geoms = snap_batch(new_points.geometry.tolist(), streets, street_index)
        # Strictly keep only valid shapely Points
        cleaned_geoms = []
        for g in snapped_geoms:
            if isinstance(g, Point):
                cleaned_geoms.append(g)
            else:
                # print(f"Invalid geometry type: {type(g)} - {g}")
                cleaned_geoms.append(None)
        new_points = new_points.copy()
        new_points["geometry"] = cleaned_geoms
        new_points = new_points.dropna(subset=["geometry"])
        new_points = gpd.GeoDataFrame(new_points, geometry="geometry", crs=streets.crs)

        # Spatial join to find street segment
        streets["buffered_geom"] = streets.geometry.buffer(1)
        new_joined = gpd.sjoin(new_points, streets.set_geometry("buffered_geom"), predicate="intersects")
        new_map = new_joined[["point_uid", "geometry", "index_right"]].copy()

        # Combine + Save to cache (as CSV with WKT)
        full_map = pd.concat([cached_map, new_map], ignore_index=True).drop_duplicates("point_uid")
        full_map["geometry_wkt"] = full_map["geometry"].apply(lambda g: g.wkt)
        full_map.drop(columns=["geometry"], inplace=True)
        full_map.to_csv(cache_path, index=False)
        print(f"Cache updated: {cache_path}")
        # breakpoint()
    else:
        # Reload full cache
        full_map = cached_map.copy()
        full_map["geometry_wkt"] = full_map["geometry"].apply(lambda g: g.wkt)
        full_map.drop(columns=["geometry"], inplace=True)

    # === Merge points with snapped geometry and index
    full_map["geometry"] = full_map["geometry_wkt"].apply(wkt.loads)
    full_map = gpd.GeoDataFrame(full_map, geometry="geometry", crs=streets.crs)
    merged_points = points.merge(full_map, on="point_uid", how="inner")
    
    # === Final aggregation
    if "value" not in merged_points.columns:
        print("No 'value' column found — skipping aggregation.")
        return streets

    # === Aggregate per street
    if is_accident:
        agg_data = merged_points.groupby("index_right").agg(
            **{
            f"list_{city}_accidents":("value", list),
            f"sum_{city}_accidents": ("value", "sum")
            }
        ).reset_index()
    else:
        agg_data = merged_points.groupby("index_right").agg(
            **{
                f"list_{sensor_name}": ("value", list),
                f"avg_{sensor_name}": ("value", "mean")
            }
        ).reset_index()

    # === Merge into streets
    streets = streets.merge(agg_data, left_index=True, right_on="index_right", how="left")
    streets.drop(columns=["index_right", "buffered_geom"], errors="ignore", inplace=True)
    
    return streets

def process_city(city):
    """
    Main function to process all sensor files for a city.
    """
    if city not in city_data:
        raise ValueError(f"City '{city}' not found in city_data dictionary!")
    
    city_info = city_data[city]
    sensor_files = city_info["sensor_files"]
    osm_path = city_data[city]["osm_file"]
    
    # Check if file exists; fallback if not
    if not os.path.exists(osm_path):
        fallback_path = osm_path.replace("./tracks/", "/app/tracks/")
        if os.path.exists(fallback_path):
            osm_path = fallback_path
        else:
            raise FileNotFoundError(f"OSM file not found at {osm_path} or fallback {fallback_path}")

    # Now read the file
    streets = gpd.read_file(osm_path).to_crs(epsg=32637)
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
        streets = process_sensor_file(city, sensor_file, streets, street_index)

    # Select final columns
    keep_columns = [
        "id", "@id", "cycleway", "description:name", "lanes", "maxspeed",
        "sidewalk", "surface", "geometry"
    ]
    
    # Add aggregated sensor columns
    for sensor_file in city_data[city]["sensor_files"]:
        sensor_name = os.path.basename(sensor_file).replace(".geojson", "")
        keep_columns.extend([f"list_{sensor_name}", f"avg_{sensor_name}"])
    
    # Only add accident sum once if it's present
    if f"sum_{city}_accidents" in streets.columns:
        keep_columns.append(f"sum_{city}_accidents")

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