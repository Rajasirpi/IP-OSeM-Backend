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

# def osm_segments_processing_view(request, city):
#     if request.method == 'GET':
#         try:
#             message = process_city(city)
#             return JsonResponse({"status": "success", "message": message})
#         except Exception as e:
#             return JsonResponse({"status": "error", "message": str(e)}, status=500)

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
                # print(f"Invalid geometry: {geometry}")
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
    # shapely.remove_repeated_points(current_segment["coordinates"], tolerance=0)
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
        
    # breakpoint()
    base_path = '/app/tracks/tracks' if os.path.exists('/app') else './tracks/tracks'
    # Create the directory if it doesn't exist
    os.makedirs(base_path, exist_ok=True)

    tracks_path = os.path.join(base_path, f'Processed_tracks_{city}.geojson')

    # with open(tracks_path, 'w') as geojson_file:
    #     json.dump(feature_collection, geojson_file, indent=2)
    filtered_gdf.to_file(tracks_path, driver="GeoJSON")
    print ("Data processed successfully. Check the tracks folder for the processed data.")
    return JsonResponse({"status": "Data processed successfully. Check the tracks folder for the processed data."})

    # return JsonResponse({"error": "Invalid HTTP method. Only GET is allowed."}, status=405)


   
#   657b28637db43500079d749d incorrect

def preprocessing_sensors(city):

    # sensor_data = SensorDataTable.objects.filter(city = 'ms', sensor_title ='Finedust PM1') 
    # count = sensor_data.count()
    # print(f"Number of sensor data for 'ms': {count}")

    # feature_collection = {
    #     "type": "FeatureCollection",
    #     "features": []
    # }
    # seen_features = set()

    # for items in sensor_data:
    #     value = items.value
    #     s_id = items.sensor_id
    #     id = items.box_id
    #     if value:
    #         # breakpoint()
    #         for entry in value:
    #             coordinates = tuple(entry["location"])  # Convert coordinates to a tuple for immutability
    #             feature_value = float(entry["value"])
    #             timestamp = entry["createdAt"]

    #             # Create a unique identifier for the feature
    #             feature_id = (coordinates, feature_value, timestamp, s_id.sensor_id, id.box_id)

    #             # Check if the feature is already added
    #             if feature_id not in seen_features:
    #                 feature = {
    #                     "type": "Feature",
    #                     "geometry": {
    #                         "type": "Point",
    #                         "coordinates": coordinates
    #                     },
    #                     "properties": {
    #                         "value": feature_value,
    #                         "timestamp": timestamp,
    #                         "sensor_id": s_id.sensor_id,
    #                         "box_id": id.box_id
    #                     }
    #                 }
    #                 # Add the feature to the collection and mark it as seen
    #                 feature_collection["features"].append(feature)
    #                 seen_features.add(feature_id)

    # base_path = '/app/tracks' if os.path.exists('/app') else './tracks/sensor_data'
    # tracks_path = os.path.join(base_path, f'{sensor_data.sensor_title}+ _ +{sensor_data.city } +.geojson')

    # with open(tracks_path, 'w') as geojson_file:
    #     json.dump(feature_collection, geojson_file, indent=4)

    # if request.method == 'GET':
        # List of cities and sensor titles to filter
        # cities = ['ms']
        # sensor_titles = [
        #     'Finedust PM1', 'Finedust PM10', 'Finedust PM2.5',
        #     'Finedust PM4', 'Temperature', 'Rel. Humidity',
        #     'Overtaking Distance', 'Surface Anomaly'
        # ]

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
        "Geschwindigkeit":"Speed",
    }

    base_path = '/app/tracks/sensor_data' if os.path.exists('/app') else './tracks/sensor_data'
    os.makedirs(base_path, exist_ok=True)  # Ensure the directory exists

    # breakpoint()
    # Iterate over each city and sensor title
    # for city in cities:
    for sensor_title in set(SENSOR_TITLE_MAPPING.values()):
        # Fetch data filtered by city and sensor title
        mapped_titles = [title for title, mapped_title in SENSOR_TITLE_MAPPING.items() if mapped_title == sensor_title]
        sensor_data = SensorDataTable.objects.filter(city=city, sensor_title__in=mapped_titles)

        # sensor_data = SensorDataTable.objects.filter(city=city, sensor_title=sensor_title)
        count = sensor_data.count()
        print(f"Number of sensor data for '{sensor_title}' in '{city}': {count}")
        # breakpoint()
        feature_collection = {
            "type": "FeatureCollection",
            "features": []
        }
        seen_features = set()

        # Process each sensor data item
        for items in sensor_data:
            value = items.value
            s_id = items.sensor_id
            id = items.box_id
            if value:
                for entry in value:
                    coordinates = tuple(entry["location"])  # Convert coordinates to a tuple for immutability
                    feature_value = float(entry["value"])
                    timestamp = entry["createdAt"]

                    # Create a unique identifier for the feature
                    feature_id = (coordinates, feature_value, timestamp, s_id.sensor_id, id.box_id)

                    # Check if the feature is already added
                    if feature_id not in seen_features:
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": coordinates
                            },
                            "properties": {
                                "value": feature_value,
                                "timestamp": timestamp,
                                "sensor_id": s_id.sensor_id,
                                "box_id": id.box_id
                            }
                        }
                        # breakpoint()
                        # Add the feature to the collection and mark it as seen
                        feature_collection["features"].append(feature)
                        seen_features.add(feature_id)
        # breakpoint()
        # Skip writing empty GeoJSON files
        if not feature_collection["features"]:
            print(f"No valid data for '{sensor_title}' in '{city}'. Skipping.")
            continue

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
            

        # Generate the GeoJSON file name
        safe_sensor_title = sensor_title.replace(" ", "_").replace(".", "_").replace("/", "_")
        geojson_filename = f"{city}_{safe_sensor_title}.geojson"
        tracks_path = os.path.join(base_path, geojson_filename)

        # Write the feature collection to a GeoJSON file
        # with open(tracks_path, 'w') as geojson_file:
        #     json.dump(feature_collection, geojson_file, indent=4)
        #     print(f"File created: {tracks_path}")
        filtered_gdf.to_file(tracks_path, driver="GeoJSON")

    print ("sensor Data processed successfully check the sensor data folder in local for the processed data")
    return JsonResponse ({"status": "sensor Data processed successfully check the sensor data folder in local for the processed data"})


# def bikeability(request, city ):
   
#     if request.method == 'GET': 
#         # Load all sensor data
#         sensor_files = [
#             "./tracks/sensor_data/ms_Finedust_PM2_5.geojson",
#             "./tracks/sensor_data/ms_Finedust_PM10.geojson",
#             "./tracks/sensor_data/ms_Finedust_PM4.geojson",
#             "./tracks/sensor_data/ms_Finedust_PM1.geojson",
#             "./tracks/sensor_data/ms_Overtaking_Distance.geojson",
#             "./tracks/sensor_data/ms_Rel__Humidity.geojson",
#             "./tracks/sensor_data/ms_Surface_Anomaly.geojson",
#             "./tracks/sensor_data/ms_Temperature.geojson"

#         ]

#         # Assign weights for each pollutant 
#         weights = {
#             "ms_Finedust_PM25": 0.111,
#             "ms_Finedust_PM10": 0.111,
#             "ms_Finedust_PM4": 0.111,
#             "ms_Finedust_PM1": 0.111,
#             "ms_Overtaking_Distance": 0.111,
#             "ms_Rel_Humidity": 0.111,
#             "ms_Surface_Anomaly": 0.111,
#             "ms_Temperature": 0.111
#         }

#         # Initialize an empty DataFrame to store normalized values for each pollutant
#         normalized_sensor_data = pd.DataFrame()

#         # Process each sensor file
#         for file in sensor_files:
#             # Load GeoJSON file
#             data = gpd.read_file(file)
            
#             # Extract date from timestamp
#             data["date"] = pd.to_datetime(data["timestamp"]).dt.date
            
#             # Normalize the 'value' column within each date and box_id group
#             data["value_normalized"] = data.groupby(["box_id", "date"])["value"].transform(
#                 lambda x: (x - x.min()) / (x.max() - x.min()) if len(x) > 1 else x
#             )
#             # breakpoint()
#             # Add the pollutant weight to the data
#             pollutant_name = file.split("/")[-1].replace(".geojson", "")  # Extract pollutant name (e.g., PM25, PM10)
#             data["weighted_value"] = data["value_normalized"] * weights[pollutant_name]
            
#             # Append to the normalized_sensor_data DataFrame
#             normalized_sensor_data = pd.concat([normalized_sensor_data, data], ignore_index=True)

#         # breakpoint()

#         # Group sensor data by box_id and date to aggregate weighted values
#         aggregated_data = normalized_sensor_data.groupby(["box_id", "date"]).agg({
#             "weighted_value": "sum"  # Sum of weighted normalized values for all pollutants
#         }).reset_index()

#         # Add a bikeability factor score
#         aggregated_data.rename(columns={"weighted_value": "factor_score"}, inplace=True)

#         # Normalize factor_score to range [0, 1]
#         def normalize(series):
#             return (series - series.min()) / (series.max() - series.min()) if len(series) > 1 else series

#         aggregated_data["factor_score"] = normalize(aggregated_data["factor_score"])

#         # Load the routes GeoJSON file
#         routes = gpd.read_file('./tracks/Processed_tracks_ms.geojson')

#         # Convert route timestamps to datetime and extract date
#         routes["date"] = pd.to_datetime(routes["date"]).dt.date

#         # Merge the bikeability factor score with routes based on box_id and date
#         routes = routes.merge(
#             aggregated_data,
#             on=["box_id", "date"],
#             how="left"
#         )

#         # Save updated routes to a new GeoJSON file
#         routes.to_file("./tracks/routes_with_bikeability_datewise.geojson", driver="GeoJSON")

#     return {"status": "BI Analysis successfull, check for routes_with_bikeability_datewise in tracks directory"} 


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
            # "extracted_traffic_data_os":0.111,
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

    # aggregated_data = normalized_sensor_data.groupby(["box_id", "date"]).agg({
    #     "weighted_value": lambda x: x.prod()  # Multiplicative model
    # }).reset_index()

    # aggregated_data["factor_score_additive"] = aggregated_data["weighted_value"].sum()
    # aggregated_data["factor_score_multiplicative"] = (aggregated_data["weighted_value"] + 1).prod()

    
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
    # breakpoint()
    base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    os.makedirs(base_path, exist_ok=True)
    # Save updated routes to a new GeoJSON file
    output_file = f"routes_with_bikeability_{city}.geojson"
    output_path = os.path.join(base_path, output_file)

    routes.to_file(output_path, driver="GeoJSON")
    
    routes["date"] = routes["date"].astype(str)
    # Convert GeoDataFrame to JSON string
    # routes_json_str = routes.to_json()
    # # Convert JSON string to dictionary
    # routes_json_dict = json.loads(routes_json_str)

    # return JsonResponse (routes_json_dict, safe=False)
    print(f"BI Analysis successful, check for {output_file} in the BI folder in tracks directory")
    return JsonResponse ({"status": f"BI Analysis successful, check for {output_file} in the BI folder in tracks directory"})


def bikeability(request, city):
    if request.method != 'GET':
        return {"status": "Invalid request method"}
    
    # file_path = f"./tracks/BI/bikeability_{city}.geojson"
    file_path = f"./tracks/BI/routes_with_bikeability_{city}.geojson"
    try:
        with open(file_path, "r") as geojson_file:
            routes = json.load(geojson_file)  # Correct way to read a JSON file
        return JsonResponse(routes, safe=False)  # `safe=False` allows lists to be returned
    except FileNotFoundError:
        return JsonResponse({"error": "GeoJSON file not found"}, status=404)
    
def osm_bikeability(request, city):
    if request.method != 'GET':
        return {"status": "Invalid request method"}
    
    file_path = f"./tracks/BI/osm_BI_{city}.geojson"
    try:
        with open(file_path, "r") as geojson_file:
            routes = json.load(geojson_file)  # Correct way to read a JSON file
        return JsonResponse(routes, safe=False)  # `safe=False` allows lists to be returned
    except FileNotFoundError:
        return JsonResponse({"error": "GeoJSON file not found"}, status=404)

def anonymization(request, city):
    if request.method != 'GET':
        return {"status": "Invalid request method"}
    
    file_path = f"./tracks/anonymization/res_{city}.geojson"
    try:
        with open(file_path, "r") as geojson_file:
            routes = json.load(geojson_file)  # Correct way to read a JSON file
        return JsonResponse(routes, safe=False)  # `safe=False` allows lists to be returned
    except FileNotFoundError:
        return JsonResponse({"error": "GeoJSON file not found"}, status=404)



# def normalize_weights(category_weights):
#     total = sum(category_weights.values())
#     if total == 0:
#         raise ValueError("At least one category weight must be greater than 0.")
    
#     return {k: v / total for k, v in category_weights.items()}


def expand_weights(category_weights):
    category_weights = {k.lower(): v for k, v in category_weights.items()}
    # normalized = normalize_weights(category_weights)
    # breakpoint()
    CATEGORY_SENSORS = {
        "safety": ["Overtaking_Distance", "Speed"],
        "infrastructure_quality": ["Surface_Anomaly"],
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
    # breakpoint()
    return final_weights


@csrf_exempt
def osm_segements_bikeability_index_view(request, city):
    # Default weights in case they are not provided in the request
    default_weights = {
        "safety": 0.222,  # Corresponds to Safety
        "infrastructure_quality": 0.111,  # Corresponds to Infrastructure
        "environment_quality": 0.666  # Corresponds to Environment
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
            weights = expand_weights(weights)
            # print(weights)
            
            # Calculate bikeability index using the provided or default weights
            response_data = calculate_bikeability(city, weights)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON in request body"}, status=400)
    else:  # For GET requests, use default weights
        weight = default_weights
        weights = expand_weights(weight)
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
    "Surface_Anomaly": {"type": "linear_cost", "min": 0, "max": 10},  # ISO: >10 mm = rough
    "Overtaking_Distance": {"type": "linear_benefit", "min": 1.0, "max": 2.0},  # ≥1.5m is ideal
    "Temperature": {"type": "triangular", "min": 10, "opt": 22, "max": 30},     # comfort zone
    "Rel__Humidity": {"type": "triangular", "min": 20, "opt": 50, "max": 70},   # comfort 40–60%
    "Speed": {"type": "linear_cost", "min": 10, "max": 50},  # <30 ideal, >50 dangerous
}

def normalize_semantic(series, sensor_name, config):
    if series.isnull().all():
        return series  # Return as is if all values are missing

    params = config.get(sensor_name)
    if not params:
        print(f"No config found for sensor: {sensor_name}")
        return series
    # breakpoint()
    s = series.copy()
    typ = params["type"]

    if typ == "linear_benefit":
        # higher = better
        norm = (s - params["min"]) / (params["max"] - params["min"])
        print(sensor_name,typ)
    elif typ == "linear_cost":
        # lower = better
        norm = (params["max"] - s) / (params["max"] - params["min"])
        print(sensor_name,typ)
    elif typ == "triangular":
        min_val, opt_val, max_val = params["min"], params["opt"], params["max"]
        norm = s.apply(lambda x: 0 if x <= min_val or x >= max_val else
                       (x - min_val) / (opt_val - min_val) if x <= opt_val else
                       (max_val - x) / (max_val - opt_val))
        print(sensor_name,typ)
    else:
        raise ValueError(f"Unknown normalization type for {sensor_name}")

    return norm.clip(0, 1)


def calculate_bikeability(city,  weights=None):
    """
    Calculates bikeability index for streets using existing processed data.
    """
    # weights = {
    #     "weights": {
    #         "ms_Finedust_PM2_5": 0.111,
    #         "ms_Finedust_PM10": 0.111,
    #         "ms_Finedust_PM4": 0.111,
    #         "ms_Finedust_PM1": 0.111,
    #         "ms_Overtaking_Distance": 0.111,
    #         "ms_Rel__Humidity": 0.111,
    #         "ms_Surface_Anomaly": 0.111,
    #         "ms_Speed": 0.111,
    #         "ms_Temperature": 0.111
    #     },
    # }
    # city_info = city_data[city]

    # If weights not passed, use default from city_data
    # if weights is None:
    #     weights = city_info["weights"]
    process_file = f"./tracks/BI/osm_streets_{city}_winter.geojson"
    # weights = city_info["weights"]
    # breakpoint()
    print('Calculates bikeability index for streets using existing processed data')
    # Load the pre-processed streets file (already has average sensor values)
    # streets = gpd.read_file(process_file).to_crs(epsg=32637)
    streets = gpd.read_file(process_file)
    # if all(k in ["Safety", "Infrastructure Quality", "Environment Quality"] for k in weights.keys()):
    
    # weight = expand_weights(weights)
    
    sensor_columns = [f"avg_{city}_{sensor}" for sensor in weights.keys()]
    existing_columns = [col for col in sensor_columns if col in streets.columns]  # Ensure only existing columns are used

    # print("Expected columns:", sensor_columns)
    # print("Available columns:", streets.columns)
    

    if not existing_columns:
        print(f"Warning: None of the expected sensor columns found in data for {city}.")
        return None

    streets_subset = streets[existing_columns]
    
    # Identify rows where all sensor values are missing
    all_null_rows = streets_subset.isnull().all(axis=1)
    
    # Fill missing values for rows where at least one sensor value exists
    for sensor in existing_columns:
        mean_value = streets[sensor].mean(skipna=True)  # Compute mean excluding NaNs
        # streets.loc[~all_null_rows & streets[sensor].isnull(), sensor] = mean_value
        streets.loc[~all_null_rows & streets[sensor].isnull(), sensor] = 0

    
    # # Normalize relevant columns
    # for sensor_name in weights.keys():
    #     if f'avg_{city}_'+ sensor_name in streets.columns:
    #         # breakpoint()
    #         streets[sensor_name + "_normalized"] = normalize(streets[f'avg_{city}_'+ sensor_name],invert=False)
    #         # print(streets[sensor_name + "_normalized"])
    #     else:
    #         print(f"Warning: {sensor_name} column missing in streets data.")

    for sensor_name in weights.keys():
        column_name = f'avg_{city}_' + sensor_name
        if column_name in streets.columns:
            norm_col = sensor_name + "_normalized"
            streets[norm_col] = normalize_semantic(streets[column_name], sensor_name, normalization_config)
        else:
            print(f"Warning: {sensor_name} column missing in streets data.")


    # breakpoint()
    # Compute weighted sum for bikeability index
    streets["bikeability_index"] = sum(
        streets[sensor + "_normalized"] * weight
        for sensor, weight in weights.items() if sensor + "_normalized" in streets
    )

    # Set bikeability_index to NaN for rows where all sensor values were missing
    streets.loc[all_null_rows, "bikeability_index"] = None

    # Compute bikeability index, ignoring NaN values
    # weighted_sum = []
    # for sensor, weight in weights.items():
    #     norm_col = sensor + "_normalized"
    #     if norm_col in streets:
    #         weighted_sum.append(streets[norm_col].fillna(0) * weight)  # Replace NaNs with 0 (no contribution)

    # if weighted_sum:
    #     streets["bikeability_index"] = np.sum(weighted_sum, axis=0)
    # else:
    #     streets["bikeability_index"] = np.nan  # No data available

    base_path = '/app/tracks/BI/' if os.path.exists('/app') else './tracks/BI/'
    os.makedirs(base_path, exist_ok=True)
    # Save updated routes to a new GeoJSON file
    output_file = f"osm_BI_{city}.geojson"
    output_path = os.path.join(base_path, output_file)
    if os.path.exists(output_path):
        os.remove(output_path)
    # Save the final result
    # output_file = f"bikeability_{city}_winter_zeros_changed_weights.geojson"
    streets = streets.dropna(subset=["bikeability_index"])
    streets_4326 = streets.to_crs(epsg=4326)
    # streets_4326.to_file(output_path, driver="GeoJSON")
    print(f"Bikeability index saved to {output_file}")
    simplified = streets_4326.copy()
    simplified["geometry"] = simplified["geometry"].simplify(tolerance=0.0001, preserve_topology=True)
    simplified.to_file(output_path, driver="GeoJSON")
    # geojson_str = streets_4326.to_json()
    geojson_str = simplified.to_json()
    geojson_dict = json.loads(geojson_str)

    # return JsonResponse ({ "message": f"OSM_BI Analysis successful, check for {output_file} in the  BI folder in tracks directory"})
    return JsonResponse(geojson_dict, safe=False)

# Run for 'ms' city using the pre-processed file
# streets_with_bikeability = calculate_bikeability("ms")