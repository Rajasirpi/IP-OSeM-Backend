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

def osm_segments_processing_view(city):
    if __name__ == "__main__":
        response_data =  process_city(city)
    return response_data

def osm_segements_bikeability_index_view(city):
    response_data = calculate_bikeability(city)
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

    return JsonResponse({"status": "Data processed successfully. Check the tracks folder for the processed data."})

    # return JsonResponse({"error": "Invalid HTTP method. Only GET is allowed."}, status=405)


   
#   657b28637db43500079d749d incorrect

def preprocessing_sensors(city):

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
        filtered_gdf.to_file(tracks_path, driver="GeoJSON")

    return JsonResponse ({"status": "sensor Data processed successfully check the sensor data folder in local for the processed data"})

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
        "process_file": "./tracks/osm_streets_ms_winter_dist.geojson"
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
        data = data[data["month"].isin([10, 11, 12])]
        
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
    routes = routes[routes["month"].isin([10, 11, 12])]

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
    # Save updated routes to a new GeoJSON file
    output_file = f"routes_with_bikeability_{city}.geojson"
    output_path = os.path.join(base_path, output_file)

    routes.to_file(output_path, driver="GeoJSON")
    
    routes["date"] = routes["date"].astype(str)
    # Convert GeoDataFrame to JSON string
    routes_json_str = routes.to_json()
    # Convert JSON string to dictionary
    routes_json_dict = json.loads(routes_json_str)

    # return JsonResponse (routes_json_dict, safe=False)
    return JsonResponse ({"status": f"BI Analysis successful, check for {output_file} in tracks directory"})


def bikeability(request, city):
    if request.method != 'GET':
        return {"status": "Invalid request method"}
    
    file_path = f"./tracks/BI/routes_with_bikeability_{city}.geojson"
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

    # Save final output
    output_file = f"osm_streets_{city}_winter_newmax.geojson"
    streets.to_file(output_file, driver="GeoJSON")
    print(f"🚀 Processing completed for {city}. Output saved to {output_file}")

# if __name__ == "__main__":
#     # Example usage
#     process_city("ms")

def normalize(series, invert=False):
    """Normalize values to range [0, 1].
    
    If `invert=True`, inverts the scale so that lower values are better (bikeability).
    """
    if len(series) <= 1:
        return series  # Return as is if only one value

    norm = (series - series.min()) / (series.max() - series.min())  # Formula 1
    return 1 - norm if invert else norm  # Formula 2 if invert=True

def calculate_bikeability(city):
    """
    Calculates bikeability index for streets using existing processed data.
    """
    city_info = city_data[city]
    weights = city_info["weights"]
    # breakpoint()
    # Load the pre-processed streets file (already has average sensor values)
    streets = gpd.read_file(city_info["process_file"]).to_crs(epsg=32637)
    sensor_columns = [f"avg_{sensor}" for sensor in weights.keys()]
    existing_columns = [col for col in sensor_columns if col in streets.columns]  # Ensure only existing columns are used

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

    
    # Normalize relevant columns
    for sensor_name in weights.keys():
        if 'avg_'+ sensor_name in streets.columns:
            # breakpoint()
            streets[sensor_name + "_normalized"] = normalize(streets['avg_'+ sensor_name],invert=False)
            # print(streets[sensor_name + "_normalized"])
        else:
            print(f"Warning: {sensor_name} column missing in streets data.")

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

    # Save the final result
    output_file = f"bikeability_{city}_winter_zeros_changed_weights.geojson"
    streets.to_file(output_file, driver="GeoJSON")
    print(f"Bikeability index saved to {output_file}")

    return streets

# Run for 'ms' city using the pre-processed file
# streets_with_bikeability = calculate_bikeability("ms")