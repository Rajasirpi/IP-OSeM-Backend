from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .utils import fetch_and_store_data
from sensebox.models import BoxTable, SensorTable, SensorDataTable, TracksTable
import json
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
import os
import geopandas as gpd
import pandas as pd
import plotly.express as px
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
from shapely.strtree import STRtree
import multiprocessing as mp
import numpy as np
import subprocess
import urllib.request
import time

def homepage(request):
    return render(request, 'homepage.html')  # Render an HTML template

def preprocessing_tracks_view(request, city):
    if request.method == 'GET':
        response_data =  preprocessing_tracks(city)
    return response_data

def preprocessing_sensors_view(request, city):
    if request.method == 'GET':
        response_data =  preprocessing_sensors(city)
    return response_data

def bikeability_trackwise_view(request, city):
    if request.method == 'GET':
        response_data =  bikeability_trackwise(city)
    return response_data


# View to fetch bike data for a city
@csrf_exempt
def fetch_bike_data(request, city):
    if request.method == 'GET':
        try:
            response_data = asyncio.run(fetch_and_store_data(city))
            return JsonResponse(response_data, safe=False, json_dumps_params={'indent': 4})
        except ValueError as err:
            return HttpResponse(str(err), status=400)
        except Exception as err:
            return HttpResponse(f"An error occurred: {err}", status=500)
    return HttpResponse("Invalid request method", status=405)


def create_feature(track_data, box_id):
    try:
        if track_data.get('type') == 'Feature' and 'geometry' in track_data:
            # breakpoint()
            geometry = track_data["geometry"]
            timestamp = track_data["properties"]["timestamps"]
            if geometry['type'] == 'LineString' and len(geometry['coordinates']) >= 2:
                return {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "box_id": box_id,
                        "timestamp":timestamp
                    }
                }
            else:
                return None
        else:
            return None  # Invalid feature
    except (KeyError, ValueError, TypeError):
        return None

def split_linestring_by_day(features,id):
    # Prepare a collection for the split features
    # split_features = []
    
    # for feature in features:
    coordinates = features["geometry"]["coordinates"]
    timestamps = features["properties"]["timestamps"]
    
    # Define the time gap threshold
    time_gap_threshold = timedelta(minutes=5)  # Example: 1 hour

    # Initialize grouping variables
    daily_segments = defaultdict(list)
    current_segment = {"coordinates": [], "timestamps": []}
    last_time = None

    # Process each coordinate and timestamp
    for coord, timestamp in zip(coordinates, timestamps):
        current_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if last_time is None:
            last_time = current_time

        # Check for time gap or date change
        if (
            current_time.date() != last_time.date()
            or current_time - last_time > time_gap_threshold
        ):
            # Save the current segment
            if len(current_segment["coordinates"]) > 2:
                daily_segments[last_time.date()].append(current_segment)
                current_segment = {"coordinates": [], "timestamps": []}

        # Add the current point to the segment
        current_segment["coordinates"].append(coord)
        current_segment["timestamps"].append(timestamp)

        # Update the last_time
        last_time = current_time
    
    # Save the last segment
    if len(current_segment["coordinates"]) > 2:
        daily_segments[last_time.date()].append(current_segment)

    feature_collection = {
        "type": "FeatureCollection",
        "features": []
    }
    for date, segments in daily_segments.items():
        for segment in segments:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": segment["coordinates"]
                },
                "properties": {
                    "timestamps": segment["timestamps"],
                    "date": str(date),
                    "box_id": id
                }
            }
            feature_collection["features"].append(feature)

    
    # Create a new FeatureCollection
    return feature_collection


def preprocessing_tracks(city):
    # if request.method == 'GET': 
    data = TracksTable.objects.filter(city = city)
    count = data.count()
    print(f"Number of tracks for {city}: {count}")
    
    # Create a single FeatureCollection with features grouped by day
    feature_collection = {
        "type": "FeatureCollection",
        "name": "Processed_feature_collection",
        "features": []
    }

    for items in data:
        track = items.tracks
        id =  items.box
        feature = split_linestring_by_day(track,id.box_id)

        # Add each feature to the main feature collection
        feature_collection["features"].extend(feature['features'])

    # Define box_ids to remove based on the city
    if city == "ms":
        ids_to_remove = {"65451cd043923100076b517c","67828c858e3d6100086a9aa1", "657b28637db43500079d749d", "66aca2c7f5b1680007e89843", "661d00531a903a0008052b78", "67226c2549d0900007c78c78"}
    elif city == "os":
        ids_to_remove = {"67529ed438b76600076d6f18"}

    # Convert the feature collection into a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(feature_collection["features"])

    # Ensure 'box_id' exists before filtering
    if "box_id" in gdf.columns:
        filtered_gdf = gdf[~gdf["box_id"].isin(ids_to_remove)]
    else:
        raise ValueError("box_id column not found in GeoJSON data.")
     
    base_path = '/app/tracks/tracks' if os.path.exists('/app') else './tracks/tracks'
    # Create the directory if it doesn't exist
    os.makedirs(base_path, exist_ok=True)

    tracks_path = os.path.join(base_path, f'Processed_tracks_{city}.geojson')

    # with open(tracks_path, 'w') as geojson_file:
    #     json.dump(feature_collection, geojson_file, indent=2)

    filtered_gdf.to_file(tracks_path, driver="GeoJSON")
    print ("Data processed successfully. Check the tracks folder for the processed data.")
    return JsonResponse({"status": "Data processed successfully. Check the tracks folder for the processed data."})


def preprocessing_sensors():
    SENSOR_TITLE_MAPPING = {
        "PM1": "Finedust PM1",
        "PM10": "Finedust PM10",
        "PM25": "Finedust PM2.5",
        "PM4": "Finedust PM4",
        "Finedust PM1": "Finedust PM1",
        "Finedust PM10": "Finedust PM10",
        "Finedust PM2.5": "Finedust PM2.5",
        "Finedust PM4": "Finedust PM4",
        "Temperature": "Temperature",
        "Rel. Humidity": "Rel. Humidity",
        "Overtaking Distance": "Overtaking Distance",
        "Surface Anomaly": "Surface Anomaly",
        "Speed": "Speed",
        "Geschwindigkeit": "Speed",
    }

    # City bounding boxes
    BBOX = {
        "ms": {"W": 7.50, "S": 51.87, "E": 7.75, "N": 52.02},
        "os": {"W": 7.85, "S": 52.19, "E": 8.17, "N": 52.37},
    }
    
    base_path = '/app/tracks/sensor_data' if os.path.exists('/app') else './tracks/sensor_data'
    os.makedirs(base_path, exist_ok=True)

    # Prepare containers: {city: {sensor_title: FeatureCollection}}
    collections = {city: {} for city in BBOX.keys()}
    seen_features = {city: set() for city in BBOX.keys()}

    # Process all mapped sensor titles
    for sensor_title in set(SENSOR_TITLE_MAPPING.values()):
        mapped_titles = [t for t, mapped in SENSOR_TITLE_MAPPING.items() if mapped == sensor_title]
        sensor_data = SensorDataTable.objects.filter(sensor_title__in=mapped_titles)

        print(f"Processing '{sensor_title}', total records: {sensor_data.count()}")

        for items in sensor_data:
            value = items.value
            s_id = items.sensor_id
            box = items.box_id

            if not value:
                continue

            for entry in value:
                lon, lat = entry["location"]
                timestamp = entry["createdAt"]
                raw_value = float(entry["value"])

                # Detect the original sensor title
                original_title = items.sensor_title

                # Convert Geschwindigkeit km/h → m/s
                if original_title == "Geschwindigkeit":
                    feature_value = raw_value / 3.6
                else:
                    feature_value = raw_value

                # Assign city by bounding box
                city_detected = None
                for city, bbox in BBOX.items():
                    if bbox["W"] <= lon <= bbox["E"] and bbox["S"] <= lat <= bbox["N"]:
                        city_detected = city
                        break

                if not city_detected:
                    continue  # skip points outside both cities

                feature_id = (lon, lat, feature_value, timestamp, s_id.sensor_id, box.box_id)
                if feature_id in seen_features[city_detected]:
                    continue

                feature = {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": (lon, lat)},
                    "properties": {
                        "value": feature_value,
                        "timestamp": timestamp,
                        "sensor_id": s_id.sensor_id,
                        "box_id": box.box_id,
                    },
                }

                # Initialize per-sensor collections if missing
                if sensor_title not in collections[city_detected]:
                    collections[city_detected][sensor_title] = {
                        "type": "FeatureCollection",
                        "features": [],
                    }

                collections[city_detected][sensor_title]["features"].append(feature)
                seen_features[city_detected].add(feature_id)

    # Save files per city & sensor
    for city, sensors in collections.items():
        for sensor_title, feature_collection in sensors.items():
            if not feature_collection["features"]:
                print(f"No valid data for '{sensor_title}' in '{city}'. Skipping.")
                continue

            gdf = gpd.GeoDataFrame.from_features(feature_collection["features"])
            gdf = gdf[gdf.is_valid & ~gdf.geometry.is_empty].dropna(subset=["geometry"])

            safe_sensor_title = sensor_title.replace(" ", "_").replace(".", "_").replace("/", "_")
            geojson_filename = f"{city}_{safe_sensor_title}.geojson"
            tracks_path = os.path.join(base_path, geojson_filename)

            gdf.to_file(tracks_path, driver="GeoJSON")
            print(f"Saved {tracks_path} with {len(gdf)} features")

    print("Sensor data processed successfully. Check the sensor_data folder.")
    return JsonResponse({"status": "Sensor data processed successfully. Check the sensor_data folder."})

# Define sensor files and weights for each city
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
        "weights": {
            "ms_Finedust_PM2_5": 0.111,
            "ms_Finedust_PM10": 0.111,
            "ms_Finedust_PM4": 0.111,
            "ms_Finedust_PM1": 0.111,
            "ms_Overtaking_Distance": 0.111,
            "ms_Rel__Humidity": 0.111,
            "ms_Surface_Anomaly": 0.111,
            "ms_Speed": 0.111,
            "ms_Temperature": 0.111
        },
        "routes_file": "./tracks/tracks/Processed_tracks_ms.geojson",
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
        "weights": {
            "os_Finedust_PM2_5": 0.111,
            "os_Finedust_PM10": 0.111,
            "os_Finedust_PM4": 0.111,
            "os_Finedust_PM1": 0.111,
            "os_Overtaking_Distance": 0.111,
            "os_Rel__Humidity": 0.111,
            "os_Surface_Anomaly": 0.111,
            "os_Speed": 0.111,
            "os_Temperature": 0.111
        },
        "routes_file": "./tracks/tracks/Processed_tracks_os.geojson",
        "osm_file": "./tracks/BI_OS.geojson"
    }
}

def bikeability_trackwise(city):
    # if request.method != 'GET':
    #     return {"status": "Invalid request method"}
    
    # Get city-specific sensor data, weights, and route file
    city_info = city_data.get(city)
    if not city_info:
        return {"status": f"City '{city}' not found in dataset"}

    sensor_files = city_info["sensor_files"]
    weights = city_info["weights"]
    routes_file = city_info["routes_file"]

    # Initialize an empty DataFrame to store normalized values for each pollutant
    normalized_sensor_data = pd.DataFrame()

    # Process each sensor file
    for file in sensor_files:
        try:
            data = gpd.read_file(file)
        except Exception as e:
            return  JsonResponse({"status": f"Error reading {file}: {e}"})

        # Extract date from timestamp
        data["date"] = pd.to_datetime(data["timestamp"]).dt.date
        data["month"] = pd.to_datetime(data["timestamp"]).dt.month

        # Filter for October, November, and December
        # data = data[data["month"].isin([10, 11, 12])]
        
        # Normalize the 'value' column within each date and box_id group
        data["value_normalized"] = data.groupby(["box_id", "date"])["value"].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if len(x) > 1 else x
        )

        # Extract pollutant name from filename
        pollutant_name = file.split("/")[-1].replace(".geojson", "")

        # Check if the pollutant exists in weights
        if pollutant_name in weights:
            data["weighted_value"] = data["value_normalized"] * weights[pollutant_name]
        else:
            return JsonResponse ({"status": f"Weight not found for {pollutant_name}"})

        # Append to the normalized_sensor_data DataFrame
        normalized_sensor_data = pd.concat([normalized_sensor_data, data], ignore_index=True)
    
    # Group sensor data by box_id and date to aggregate weighted values
    aggregated_data = normalized_sensor_data.groupby(["box_id", "date"]).agg({
        "weighted_value": "sum"   # simple additive model
    }).reset_index()

    # Normalize factor_score to range [0, 1]
    def normalize(series):
        return 1 - ((series - series.min()) / (series.max() - series.min())) if len(series) > 1 else series

    # 0 → worst bikeability
    # 1 → best bikeability
    aggregated_data["factor_score"] = normalize(aggregated_data["weighted_value"])
    
    # Load the routes GeoJSON file
    try:
        routes = gpd.read_file(routes_file)
    except Exception as e:
        return JsonResponse ({"status": f"Error reading routes file: {e}"})

    # Convert route timestamps to datetime and extract date
    routes["date"] = pd.to_datetime(routes["date"]).dt.date
    routes["month"] = pd.to_datetime(routes["date"]).dt.month

    # Filter routes data for October, November, and December
    # routes = routes[routes["month"].isin([10, 11, 12])]

    # Merge the bikeability factor score with routes based on box_id and date
    routes = routes.merge(
        aggregated_data,
        on=["box_id", "date"],
        how="left"
    )
    # Assign a unique number to each box_id
    box_id_mapping = {box: idx + 1 for idx, box in enumerate(routes["box_id"].unique())}
    routes["box_id_number"] = routes["box_id"].map(box_id_mapping)
    
    base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    os.makedirs(base_path, exist_ok=True)
    # Save updated routes to a new GeoJSON file
    output_file = f"routes_with_bikeability_{city}.geojson"
    output_path = os.path.join(base_path, output_file)

    routes.to_file(output_path, driver="GeoJSON")
    
    routes["date"] = routes["date"].astype(str)

    # return JsonResponse (routes_json_dict, safe=False)
    print(f"BI Analysis successful, check for {output_file} in the BI folder in tracks directory")
    return JsonResponse ({"status": f"BI Analysis successful, check for {output_file} in the BI folder in tracks directory"})


def expand_weights(category_weights):
    category_weights = {k.lower(): v for k, v in category_weights.items()}
    # normalized = normalize_weights(category_weights)

    CATEGORY_SENSORS = {
        "safety": ["Overtaking_Distance", "Speed", "accidents"],
        "infrastructure_quality":["cqi_index"], #"Surface_Anomaly"
        "environment_quality": [
            "Temperature", "Rel__Humidity",
            "Finedust_PM1", "Finedust_PM2_5",
            "Finedust_PM4", "Finedust_PM10"
        ],
    }

    # normalized = normalize_weights(category_weights)
    final_weights = {}

    for category, sensors in CATEGORY_SENSORS.items():
        # print(f"Processing category: {category}")
        # weight = normalized.get(category, 0)
        weight = category_weights.get(category, 0)
        # print(f"  Weight: {weight}")
        # print(f"  Sensors: {sensors}")
        if not sensors or weight == 0:
            continue
        split_weight = weight / len(sensors)
        for sensor in sensors:
            
            # for sensor_name in sensor:
            final_weights[sensor] = split_weight
    print("Final expanded weights:", final_weights)

    return final_weights


@csrf_exempt
def osm_segements_bikeability_index_view(request, city):
    # Default weights in case they are not provided in the request
    default_weights = {
        "safety": 0.4,  # Corresponds to Safety
        "infrastructure_quality": 0.5,  # Corresponds to Infrastructure
        "environment_quality": 0.1 # Corresponds to Environment
    }

    if request.method == 'POST':
        try:
            # Parse JSON body to get weights
            weights = json.loads(request.body)
            # weights = body.get("weights", {})
            if not weights:
                # If no weights are provided, use default weights
                weights = default_weights
            
            # Expand weights to sensor level
            # weights = expand_weights(weights)
            # print(weights)
            
            # Calculate bikeability index using the provided or default weights
            response_data = calculate_bikeability(city, weights)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    else:  # For GET requests, use default weights
        weight = default_weights
        # weights = expand_weights(weight)
        response_data = calculate_bikeability(city, weights)
    
    # Return the final response
    return response_data


def normalize(series, invert=False):
    """Normalize values to range [0, 1].
    
    If `invert=True`, inverts the scale so that lower values are better (bikeability).
    """
    if len(series) <= 1:
        return series  # Return as is if only one value

    norm = (series - series.min()) / (series.max() - series.min())  # Formula 1
    return 1 - norm if invert else norm  # Formula 2 if invert=True


normalization_config = {
    "Finedust_PM1": {"type": "linear_cost", "min": 0, "max": 25},
    "Finedust_PM2_5": {"type": "linear_cost", "min": 0, "max": 25},  # WHO: ≤15 24h, ≤5 annual
    "Finedust_PM4": {"type": "linear_cost", "min": 0, "max": 40},
    "Finedust_PM10": {"type": "linear_cost", "min": 0, "max": 50},   # WHO: ≤45 24h
    # "Surface_Anomaly": {"type": "linear_cost", "min": 0, "max": 10},  # ISO: >10 mm = rough
    "Overtaking_Distance": {"type": "linear_benefit", "min": 100, "max": 200},  # ≥1.5m is ideal :max value from data is 481cm
    "Temperature": {"type": "triangular", "min": 10, "opt": 22, "max": 30},     # comfort zone
    "Rel__Humidity": {"type": "triangular", "min": 20, "opt": 50, "max": 70},   # comfort 40–60%
    "Speed": {"type": "linear_cost", "min":2.77, "max": 9.722 },  # <30 ideal, >50 dangerous :max value from data is 60 : 10km/hr = 2.77 m/s and 50 km/hr = 13.88 m/s (35 km/hr = 9.722 m/s)
    "cqi_index": {"type": "linear_benefit", "min": 8, "max": 100},
    "accidents": {"type": "linear_cost", "min": 0.15, "max": 9.2},
}

def normalize_semantic(series, sensor_name, config):
    if series.isnull().all():
        return series  # Return as is if all values are missing

    params = config.get(sensor_name)
    if not params:
        print(f"No config found for sensor: {sensor_name}")
        return series
    
    s = series.copy()
    typ = params["type"]

    if typ == "linear_benefit":
        # higher = better
        norm = (s - params["min"]) / (params["max"] - params["min"])
        # print(sensor_name,typ)
    elif typ == "linear_cost":
        # lower = better
        norm = (params["max"] - s) / (params["max"] - params["min"])
        # print(sensor_name,typ)
    elif typ == "triangular":
        min_val, opt_val, max_val = params["min"], params["opt"], params["max"]
        norm = s.apply(lambda x: 0 if x <= min_val or x >= max_val else
                       (x - min_val) / (opt_val - min_val) if x <= opt_val else
                       (max_val - x) / (max_val - opt_val))
        # print(sensor_name,typ)
    else:
        raise ValueError(f"Unknown normalization type for {sensor_name}")

    return norm.clip(0, 1)

def merge_cqi(city, id_column='id', columns_to_add=None, column_rename_map=None,):

    if columns_to_add is None:
        columns_to_add = ['index', 'stress_level']

    if column_rename_map is None:
        column_rename_map = {'index': f'avg_{city}_cqi_index'}
    
    # Load both files
    gdf_index = gpd.read_file(f"./tracks/{city}_cycling_quality_index.geojson")
    gdf_sensor = gpd.read_file(f"./tracks/BI/osm_streets_{city}_winter.geojson")

    # Ensure ID columns are string for comparison
    gdf_index[id_column] = gdf_index[id_column].astype(str)
    gdf_sensor[id_column] = gdf_sensor[id_column].astype(str)

    # Check if requested columns exist
    missing_cols = [col for col in columns_to_add if col not in gdf_index.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in index file: {missing_cols}")

    # Subset and merge
    index_subset = gdf_index[[id_column] + columns_to_add].copy()
    index_subset.rename(columns=column_rename_map, inplace=True)
    merged = gdf_sensor.merge(index_subset, on=id_column, how='left')
   
    # Save merged result
    merged.to_file(f"./tracks/BI/osm_streets_{city}.geojson", driver='GeoJSON')


# def calculate_bikeability(city,  weights=None):
#     """
#     Calculates bikeability index for streets using existing processed data.
#     """
#     process_file = f"./tracks/BI/osm_streets_{city}.geojson"
  
#     print('Calculates bikeability index for streets using existing processed data')
  
#     # streets = gpd.read_file(process_file).to_crs(epsg=32637)
#     streets = gpd.read_file(process_file)

#     # weight = expand_weights(weights)
    
#     sensor_columns = [f"avg_{city}_{sensor}" for sensor in weights.keys() if sensor != "accidents"]
#     if "accidents" in weights:
#         sensor_columns.append(f"sum_{city}_accidents")   # Add accident column manually
#     existing_columns = [col for col in sensor_columns if col in streets.columns]  # Ensure only existing columns are used
    
#     if not existing_columns:
#         print(f"Warning: None of the expected sensor columns found in data for {city}.")
#         return None

#     streets_subset = streets[existing_columns]
    
#     # Identify rows where all sensor values are missing
#     all_null_rows = streets_subset.isnull().all(axis=1)
    
#     # Fill missing values for rows where at least one sensor value exists
#     for sensor in existing_columns:
#         mean_value = streets[sensor].mean(skipna=True)  # Compute mean excluding NaNs
#         # streets.loc[~all_null_rows & streets[sensor].isnull(), sensor] = mean_value
#         streets.loc[~all_null_rows & streets[sensor].isnull(), sensor] = 0

#     # === Normalize regular sensors ===
#     for sensor_name in weights.keys():
#         if sensor_name == "accidents":
#             continue  # Skip here, handle separately below

#         column_name = f'avg_{city}_' + sensor_name
#         if column_name in streets.columns:
#             norm_col = sensor_name + "_normalized"
#             streets[norm_col] = normalize_semantic(streets[column_name], sensor_name, normalization_config)
#         else:
#             print(f"Warning: {sensor_name} column missing in streets data.")

#      # === Normalize accidents ===
#     accident_col = f"sum_{city}_accidents"
#     if "accidents" in weights and accident_col in streets.columns:
#         streets["accidents_normalized"] = normalize_semantic(
#             streets[accident_col], "accidents", normalization_config
#         )

#     bikeability_sum = 0
#     for sensor, weight in weights.items():
#         # if sensor + "_normalized" in streets:
#         #     print(f"sensor: {sensor}, weight: {weight}")
#         #     bikeability_sum += streets[sensor + "_normalized"] * weight
#         if sensor == "accidents":
#             norm_col = "accidents_normalized"
#         else:
#             norm_col = sensor + "_normalized"

#         if norm_col in streets:
#             print(f"Adding sensor: {sensor}, weight: {weight}")
#             bikeability_sum += streets[norm_col] * weight

#     streets["bikeability_index"] = bikeability_sum


#     # Set bikeability_index to NaN for rows where all sensor values were missing
#     streets.loc[all_null_rows, "bikeability_index"] = None


#     base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
#     os.makedirs(base_path, exist_ok=True)
#     # Save updated routes to a new GeoJSON file
#     output_file = f"osm_BI_{city}.geojson"
#     output_path = os.path.join(base_path, output_file)
#     if os.path.exists(output_path):
#         os.remove(output_path)
#     # streets = streets.dropna(subset=["bikeability_index"])
#     streets_4326 = streets.to_crs(epsg=4326)
#     columns_to_keep = ["id", "bikeability_index", "geometry"]
#     streets_filtered = streets_4326[columns_to_keep]
#     streets_filtered.to_file(output_path, driver="GeoJSON")
#     # streets_4326.to_file(output_path, driver="GeoJSON")
#     print(f"Bikeability index saved to {output_file}")

#     # simplified = streets_4326.copy()
#     # simplified["geometry"] = simplified["geometry"].simplify(tolerance=0.0001, preserve_topology=True)
#     # simplified.to_file(output_path, driver="GeoJSON")

#     geojson_str = streets_filtered.to_json()
#     # geojson_str = simplified.to_json()
#     geojson_dict = json.loads(geojson_str)

#     return JsonResponse(geojson_dict, safe=False)



def precompute_normalized_data(city):
    process_file = f"./tracks/BI/osm_streets_{city}.geojson"
    streets = gpd.read_file(process_file)

    # Clean missing sensor values
    for col in streets.columns:
        if col.startswith(f"avg_{city}_") or col.startswith(f"sum_{city}_"):
            streets[col].fillna(0, inplace=True)

    # Apply semantic normalization to all relevant columns
    for sensor_name, config in normalization_config.items():
        avg_col = f"avg_{city}_{sensor_name}"
        sum_col = f"sum_{city}_{sensor_name}"
        if avg_col in streets:
            streets[f"{sensor_name}_normalized"] = normalize_semantic(streets[avg_col], sensor_name, normalization_config)
        elif sum_col in streets:
            streets[f"{sensor_name}_normalized"] = normalize_semantic(streets[sum_col], sensor_name, normalization_config)

    streets["safety_score"] = (
    streets["Speed_normalized"] +
    streets["Overtaking_Distance_normalized"] +
    streets["accidents_normalized"]
    ) / 3  # or weighted avg inside category if desired

    streets["infrastructure_score"] = streets["cqi_index_normalized"]

    streets["environment_score"] = (
        streets["Temperature_normalized"] +
        streets["Rel__Humidity_normalized"] +
        streets["Finedust_PM1_normalized"] +
        streets["Finedust_PM2_5_normalized"] +
        streets["Finedust_PM4_normalized"] +
        streets["Finedust_PM10_normalized"]
    ) / 6

    # Keep minimal columns
    keep_cols = [c for c in streets.columns if c.endswith("_score")] + ["id", "geometry"]
    streets = streets[keep_cols]

    # Save as Parquet (much faster than GeoJSON)
    output_path = f"./tracks/BI/osm_normalized_{city}.geojson"
    streets.to_file(output_path, index=False)
    print(f"Normalized data saved: {output_path}")

def calculate_bikeability(city, weights):
    start_time = time.time()
    # weights = {k.lower(): v for k, v in weights.items()}
    path = f"./tracks/BI/osm_normalized_{city}.geojson"
    streets = gpd.read_file(path)
    # print(weights)
    
    # Compute weighted sum directly
    # bikeability_sum = np.zeros(len(streets))
    # for sensor, weight in weights.items():
    #     col = f"{sensor}_normalized"
    #     if col in streets:
    #         bikeability_sum += streets[col].fillna(0) * weight

    # streets["bikeability_index"] = bikeability_sum

    streets["bikeability_index"] = (
        streets["safety_score"] * weights["safety"] +
        streets["infrastructure_score"] * weights["infrastructure_quality"] +
        streets["environment_score"] * weights["environment_quality"]
    )
 
    # Keep minimal columns
    output = streets[["id", "bikeability_index", "geometry"]].to_crs(epsg=4326)

    geojson_str = output.to_json()
    geojson_dict = json.loads(geojson_str)

    # base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    # os.makedirs(base_path, exist_ok=True)
    # # Save updated routes to a new GeoJSON file
    # output_file = f"osm_BI_{city}.geojson"
    # output_path = os.path.join(base_path, output_file)
    # if os.path.exists(output_path):
    #     os.remove(output_path)
    # output.to_file(output_path, driver="GeoJSON")
    
    # elapsed = time.time() - start_time
    # print(f"calculate_bikeability took {elapsed:.2f} seconds")

    return JsonResponse(geojson_dict, safe=False)

# This method creates a traffic.csv file in the work folder of the routing engine to customize it
# based on bikeability weights.
def calculate_traffic(city, weights):
    path = f"./tracks/BI/osm_normalized_{city}.geojson"
    streets = gpd.read_file(path)

    # Calculate bikeability score for each street segment
    streets["bikeability_index"] = (
        streets["safety_score"] * weights["safety"] +
        streets["infrastructure_score"] * weights["infrastructure_quality"] +
        streets["environment_score"] * weights["environment_quality"]
    )
    
    # We have to rename and adapt the column containing the way id to match the data we have in ways.csv and cast
    # way_id column to integer to merge against integer id column in ways csv
    streets["way_id"]= streets["id"].str.replace('way/', '')
    streets["way_id"]= streets["way_id"].astype(int)
    
    # Drop duplicate street segments (there should be only one bikeability score per street segment)
    streets = streets.drop_duplicates(subset=['way_id'])
    
    # Read "ways.csv", a file that matches each way with start and end nodes, as this is the information the routing
    # engine requires. Then merge the data with the bikeability index, so that we have a list of [start_node_id, end_node_id, bikeability_index]
    ways_df = pd.read_csv('./sensebox/osrm/ways.csv')
    merged_df = ways_df.merge(streets, on='way_id')

    # Adapt the speed of edges to the bikeability index (0..100) by multiplying it with 25.
    # For a street segment with bikeability 80 this returns a speed of 20km/h.
    merged_df["speed"] = merged_df["bikeability_index"]*0.25

    # Save the traffic.csv file
    output_filename = './sensebox/osrm/work/traffic.csv'
    output = merged_df[["first_node_id", "second_node_id", "speed"]].to_csv(output_filename, index=False, header=False)

def route(request):
    # Step 1: Parse Input
    start_lon = request.GET.get('start_lon')
    start_lat = request.GET.get('start_lat')
    end_lon = request.GET.get('end_lon')
    end_lat = request.GET.get('end_lat')
    # We need to parse the weights to integers so that we can use them for the computation of the bikeability scores
    infrastructure_score = int(request.GET.get('infrastructure_score', 40))
    safety_score = int(request.GET.get('safety_score', 50))
    environmental_score = int(request.GET.get('environmental_score', 10))
    weights = {
        "safety": safety_score,  # Corresponds to Safety
        "infrastructure_quality": infrastructure_score,  # Corresponds to Infrastructure
        "environment_quality": environmental_score # Corresponds to Environment
    }

    # Step 2: The following method computes and saves a traffic.csv file in the work folder
    calculate_traffic('ms', weights)
    
    # Step 3: Customize OSRM so that the routing network incorporates the bikeability-infused edge speeds
    subprocess.call(['./sensebox/osrm/osrm-customize','sensebox/osrm/work/smol.osrm', '--segment-speed-file=sensebox/osrm/work/traffic.csv'])

    # Step 4: Run the routing engine as a running process
    process = subprocess.Popen(['./sensebox/osrm/osrm-routed', '--algorithm=mld','sensebox/osrm/work/smol.osrm'])
    url = f"http://localhost:5000/route/v1/driving/{str(start_lon)},{start_lat};{end_lon},{end_lat}?overview=full&steps=true&geometries=geojson"
    # The two second timeout makes sure that the router will have loaded and is ready. TODO: Read stdout of the routing process
    # and act immediately after it has loaded. Don't wait for a fixed time.
    time.sleep(2)

    # Step 5: Request the route from the routing engine by http request
    contents = urllib.request.urlopen(url).read()
    # Decode the response from OSRM (we expect a utf-8 encoded string containing json data)
    output = contents.decode('utf-8')

    # Step 6: Terminate the routing engine - this is important so that we can start a "clean" router with a differently
    #         customized routing network on the next request.
    process.terminate()

    # Step 7: Return the response from the routing engine to the frontend. Note that we have to parse the string to a dict
    #         to re-serialize it because JsonResponse expects a dict and not a string. TODO: This should be fixed.
    return JsonResponse(json.loads(output), safe=False)
