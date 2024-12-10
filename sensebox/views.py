from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .utils import fetch_and_store_data
from sensebox.models import BoxTable, SensorTable, SensorDataTable, TracksTable
import requests
import json
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
import os
import geopandas as gpd
import pandas as pd
import plotly.express as px
from shapely.geometry import LineString


def homepage(request):
    # datapreprocessing_tracks()
    # preprocessing_sensors()
    # Agg_tracks()
    return render(request, 'homepage.html')  # Render an HTML template

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


def preprocessing_tracks(request, city):
    if request.method == 'GET': 
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

        
        breakpoint()
        base_path = '/app/tracks' if os.path.exists('/app') else './tracks'
        tracks_path = os.path.join(base_path, f'Processed_tracks_{city}.geojson')

        with open(tracks_path, 'w') as geojson_file:
            json.dump(feature_collection, geojson_file, indent=2)

        return JsonResponse({"status": "Data processed successfully. Check the tracks folder for the processed data."})

    return JsonResponse({"error": "Invalid HTTP method. Only GET is allowed."}, status=405)


   
def Agg_tracks():
    data = TracksTable.objects.filter(city = 'ms') 
    # feature_collection = {"type": "FeatureCollection", "features": []}

    # for items in data:
    #     track = items.tracks
    #     id =  items.box   
    #     # timestamp = items.timestamp
        
    #     # if track['geometry']['type'] == 'LineString':
    #     geojson_feature = create_feature(track, id.box_id)
        
    #     if geojson_feature:
    #         feature_collection['features'].append(geojson_feature)
    # # print(feature_collection)
    # breakpoint()

    # base_path = '/app/tracks' if os.path.exists('/app') else './tracks'
    # tracks_path = os.path.join(base_path, 'Agg_tracks_ms.geojson')

    # # Write feature collection to GeoJSON file
    # with open(tracks_path, 'w') as geojson_file:
    #     json.dump(feature_collection, geojson_file, indent=2)

    # Create an output path
    base_path = '/app/tracks' if os.path.exists('/app') else './tracks'
    geopackage_path = os.path.join(base_path, 'Agg_tracks_ms.gpkg')
    
    # Remove the existing GeoPackage if it exists
    if os.path.exists(geopackage_path):
        os.remove(geopackage_path)
    
    # Iterate over the tracks in the database
    for idx, item in enumerate(data, start=1):
        track = item.tracks
        box_id = item.box.box_id
        # breakpoint()
        # Extract geometry from the track
        if track.get('geometry', {}).get('type') == 'LineString':
            coordinates = track['geometry']['coordinates']
            timestamps = track['properties']['timestamps']

            # Skip invalid LineStrings with no coordinates or less than two points
            if not coordinates or len(coordinates) < 2:
                print(f"Skipping invalid track for box_id {box_id} with insufficient coordinates.")
                continue
            
            # Create a Shapely LineString
            try:
                geometry = LineString(coordinates)
            except Exception as e:
                print(f"Error creating LineString for box_id {box_id}: {e}")
                continue
            
            # Create a GeoDataFrame for this track
            gdf = gpd.GeoDataFrame(
                [{'box_id': box_id,
                  'timestamps': timestamps}],  # Add attributes
                geometry=[geometry],  # Add geometry
                crs='EPSG:4326'  # Coordinate Reference System
            )
            
            # Save this track as a new layer in the GeoPackage
            layer_name = f'track_{idx}_box_{box_id}'
            gdf.to_file(geopackage_path, layer=layer_name, driver='GPKG')
    
    print(f"GeoPackage created: {geopackage_path}")


#   657b28637db43500079d749d incorrect

def preprocessing_sensors(request, city):

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
    if request.method == 'GET':
        # List of cities and sensor titles to filter
        # cities = ['ms']
        sensor_titles = [
            'Finedust PM1', 'Finedust PM10', 'Finedust PM2.5',
            'Finedust PM4', 'Temperature', 'Rel. Humidity',
            'Overtaking Distance', 'Surface Anomaly'
        ]

        base_path = '/app/tracks/sensor_data' if os.path.exists('/app') else './tracks/sensor_data'
        os.makedirs(base_path, exist_ok=True)  # Ensure the directory exists

        # Iterate over each city and sensor title
        # for city in cities:
        for sensor_title in sensor_titles:
            # Fetch data filtered by city and sensor title
            sensor_data = SensorDataTable.objects.filter(city=city, sensor_title=sensor_title)
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
                            # Add the feature to the collection and mark it as seen
                            feature_collection["features"].append(feature)
                            seen_features.add(feature_id)

            # Skip writing empty GeoJSON files
            # if not feature_collection["features"]:
            #     print(f"No valid data for '{sensor_title}' in '{city}'. Skipping.")
            #     continue

            # Generate the GeoJSON file name
            safe_sensor_title = sensor_title.replace(" ", "_").replace(".", "_").replace("/", "_")
            geojson_filename = f"{city}_{safe_sensor_title}.geojson"
            tracks_path = os.path.join(base_path, geojson_filename)

            # Write the feature collection to a GeoJSON file
            with open(tracks_path, 'w') as geojson_file:
                json.dump(feature_collection, geojson_file, indent=4)

    return {"status": "sensor Data processed successfully check the sensor data folder in local for the processed data"}


def bikeability(request):
   
    if request.method == 'GET': 
        # Load all sensor data
        sensor_files = [
            "./tracks/sensor_data/ms_Finedust_PM25.geojson",
            "./tracks/sensor_data/ms_Finedust_PM10.geojson",
            "./tracks/sensor_data/ms_Finedust_PM4.geojson",
            "./tracks/sensor_data/ms_Finedust_PM1.geojson",
            "./tracks/sensor_data/ms_Overtaking_Distance.geojson",
            "./tracks/sensor_data/ms_Rel_Humidity.geojson",
            "./tracks/sensor_data/ms_Surface_Anomaly.geojson",
            "./tracks/sensor_data/ms_Temperature.geojson"

        ]

        # Assign weights for each pollutant 
        weights = {
            "ms_Finedust_PM25": 0.125,
            "ms_Finedust_PM10": 0.125,
            "ms_Finedust_PM4": 0.125,
            "ms_Finedust_PM1": 0.125,
            "ms_Overtaking_Distance": 0.125,
            "ms_Rel_Humidity": 0.125,
            "ms_Surface_Anomaly": 0.125,
            "ms_Temperature": 0.125
        }

        # Initialize an empty DataFrame to store normalized values for each pollutant
        normalized_sensor_data = pd.DataFrame()

        # Process each sensor file
        for file in sensor_files:
            # Load GeoJSON file
            data = gpd.read_file(file)
            
            # Extract date from timestamp
            data["date"] = pd.to_datetime(data["timestamp"]).dt.date
            
            # Normalize the 'value' column within each date and box_id group
            data["value_normalized"] = data.groupby(["box_id", "date"])["value"].transform(
                lambda x: (x - x.min()) / (x.max() - x.min()) if len(x) > 1 else x
            )
            # breakpoint()
            # Add the pollutant weight to the data
            pollutant_name = file.split("/")[-1].replace(".geojson", "")  # Extract pollutant name (e.g., PM25, PM10)
            data["weighted_value"] = data["value_normalized"] * weights[pollutant_name]
            
            # Append to the normalized_sensor_data DataFrame
            normalized_sensor_data = pd.concat([normalized_sensor_data, data], ignore_index=True)

        # breakpoint()

        # Group sensor data by box_id and date to aggregate weighted values
        aggregated_data = normalized_sensor_data.groupby(["box_id", "date"]).agg({
            "weighted_value": "sum"  # Sum of weighted normalized values for all pollutants
        }).reset_index()

        # Add a bikeability factor score
        aggregated_data.rename(columns={"weighted_value": "factor_score"}, inplace=True)

        # Normalize factor_score to range [0, 1]
        def normalize(series):
            return (series - series.min()) / (series.max() - series.min()) if len(series) > 1 else series

        aggregated_data["factor_score"] = normalize(aggregated_data["factor_score"])

        # Load the routes GeoJSON file
        routes = gpd.read_file('./tracks/Processed_tracks_ms.geojson')

        # Convert route timestamps to datetime and extract date
        routes["date"] = pd.to_datetime(routes["date"]).dt.date

        # Merge the bikeability factor score with routes based on box_id and date
        routes = routes.merge(
            aggregated_data,
            on=["box_id", "date"],
            how="left"
        )

        # Save updated routes to a new GeoJSON file
        routes.to_file("./tracks/routes_with_bikeability_datewise.geojson", driver="GeoJSON")

    return {"status": "BI Analysis successfull, check for routes_with_bikeability_datewise in tracks directory"} 