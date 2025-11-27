
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

    
    base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    os.makedirs(base_path, exist_ok=True)

    # Save updated routes to a new GeoJSON file
    output_file = f"osm_streets_{city}_winter.geojson"
    output_path = os.path.join(base_path, output_file)
   
    streets.to_file(output_path, driver="GeoJSON")
    print(f"Processing completed for {city}. Output saved to {output_file}")

    return JsonResponse({ "message": f"Processing completed for {city}. Output saved to {output_file}"})

