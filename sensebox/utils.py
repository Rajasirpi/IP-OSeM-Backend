import requests
from sensebox.models import BoxTable, SensorTable, SensorDataTable, TracksTable
import json
import os
from django.db.models import Count
from django.db import transaction
import asyncio
import aiohttp
from asgiref.sync import sync_to_async

# Helper function to create a feature from track data
def create_feature(track_data, box_id, timestamp):
    try:
        if track_data.get('type') == 'Feature' and 'geometry' in track_data:
            geometry = track_data['geometry']
            if geometry['type'] == 'LineString' and len(geometry['coordinates']) >= 2:
                return {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "box_id": box_id,
                        "timestamp": timestamp
                    }
                }
            else:
                # print(f"Invalid geometry: {geometry}")
                return None
        else:
            return None  # Invalid feature
    except (KeyError, ValueError, TypeError):
        return None

 
# def fetch_and_store_data(city):

#     if os.path.exists('/app'):  # Check if running in Docker
#         base_path = '/app/tracks'
#     else:  # Running locally
#         base_path = './tracks'

#     if city == "ms":
#         MeasurementModel = BikeMeasurementMS
#         TrackModel = BikeTrackMS
#         TrackMeasurementModel =  BikeTrackMeasurementMS

#         tracks_path = os.path.join(base_path, 'bike_tracks_feature_collection_ms.geojson')
#     elif city == "os":
#         MeasurementModel = BikeMeasurementOS
#         TrackModel = BikeTrackOS
#         TrackMeasurementModel =  BikeTrackMeasurementOS
#         tracks_path = os.path.join(base_path, 'bike_tracks_feature_collection_os.geojson')
#     else:
#         raise ValueError("City not supported")
    
#     base_url = "https://api.opensensemap.org/boxes"
    
#     # Define bbox values for the given cities
#     bbox = None
#     if city == "os":
#         bbox = {"W": "7.85", "S": "52.19", "E": "8.17", "N": "52.37"}
#     elif city == "ms":
#         bbox = {"W": "7.50", "S": "51.87", "E": "7.75", "N": "52.02"}
#     else:
#         raise ValueError("City not supported.")
    
#     bbox_str = f"{bbox['W']},{bbox['S']},{bbox['E']},{bbox['N']}"
#     # last_fetch_time = get_last_fetch_time(city)

#     # if last_fetch_time is None:
#     #     last_fetch_time = datetime.now() - timedelta(days=7)

#     # params = {"grouptag": "bike", "bbox": bbox_str,"from-date": last_fetch_time.strftime("%Y-%m-%dT%H:%M:%SZ")}
#     params = {"grouptag": "bike", "bbox": bbox_str}
#     # breakpoint()
#     response = requests.get(base_url, params=params)
#     response.raise_for_status()
#     data = response.json()
#     # print(data)
#     id_list = [{"id": entry["_id"], "timestamp": entry["currentLocation"]["timestamp"]} for entry in data]

#     # Clear existing records before inserting new ones
#     # MeasurementModel.objects.all().delete()
#     # TrackModel.objects.all().delete()
#     # TrackMeasurementModel.objects.all().delete()

#     feature_collection = {"type": "FeatureCollection", "features": []}

#     for entry in id_list:
#         id = entry["id"]
#         timestamp = entry["timestamp"]

#         # Fetch detailed box measurements
#         measurements_url = f"https://api.opensensemap.org/boxes/{id}"
#         measurements_res = requests.get(measurements_url)
#         measurements = measurements_res.json()

#         box_id = measurements["_id"]
#         name = measurements.get("name")
#         updated_at = measurements.get("updatedAt")
#         created_at = measurements.get("createdAt")
#         last_measurement_at = measurements.get("lastMeasurementAt")
#         lonlat = measurements.get("currentLocation")
#         coordinates = lonlat.get('coordinates')

#         # Loop through sensors and extract measurements
#         for sensor in measurements["sensors"]:
#             sensor_id = sensor.get("_id")
#             sensor_icon = sensor.get("icon")
#             sensor_title = sensor.get("title")
#             sensor_unit = sensor.get("unit")
#             sensor_type = sensor.get("sensorType")
#             last_measurement = sensor.get("lastMeasurement", {})
#             sensor_value = last_measurement.get("value")

#             defaults = {
#                 'name': name,
#                 'sensor_id':sensor_id,
#                 'sensor_icon': sensor_icon,
#                 'sensor_unit': sensor_unit,
#                 'sensor_type': sensor_type,
#                 'sensor_value': sensor_value,
#                 'created_at': created_at,
#                 'last_measurement_at': last_measurement_at,
#                 'location': coordinates
#             }

#             MeasurementModel.objects.update_or_create(
#                 box_id=box_id,
#                 sensor_title=sensor_title,  # Assuming you want to use sensor_title as a unique identifier
#                 updated_at=updated_at,
#                 defaults=defaults
#             )

#             # Save data using Django ORM
#             # MeasurementModel.objects.create(
#             #     box_id=box_id,
#             #     name=name,
#             #     sensor_id=sensor_id,
#             #     sensor_icon=sensor_icon,
#             #     sensor_title=sensor_title,
#             #     sensor_unit=sensor_unit,
#             #     sensor_type=sensor_type,
#             #     sensor_value=sensor_value,
#             #     updated_at=updated_at,
#             #     created_at=created_at,
#             #     last_measurement_at=last_measurement_at,
#             #     location=coordinates
#             # )
            

#             tracks_data_url = f'https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?to-date={timestamp}'
#             tracks_data = requests.get(tracks_data_url)
#             sensor_values = tracks_data.json()
        
           
#             TrackMeasurementModel.objects.update_or_create(
#                 box_id=box_id,
#                 sensor_id=sensor_id,
#                 defaults={'measurements': sensor_values }
#             )

#             # TrackMeasurementModel.objects.create(
#             #     box_id=box_id,
#             #     sensor_id=sensor_id,
#             #     measurements=sensor_values 
#             # )
            
#         # Fetch and store bike tracks
#         route_url = f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&to-date={timestamp}"
#         route = requests.get(route_url)
#         track = route.json()

#         # Save track data using Django ORM
#         TrackModel.objects.update_or_create(
#             box_id=id,
#             timestamp=timestamp,
#             defaults={'track_data':track}
#         )

#         # TrackModel.objects.create(
#         #     box_id=id,
#         #     track_data=track,
#         #     timestamp=timestamp
#         # )

#         # Create feature for track data (assuming `create_feature` is a defined helper)
#         feature = create_feature(track, id, timestamp)
#         if feature:
#             feature_collection['features'].append(feature)


#     # breakpoint()

#     # Define the path based on the environment
#     os.makedirs(base_path, exist_ok=True)
#     # tracks_path = '/app/tracks/bike_tracks_feature_collection.geojson' if os.path.exists('/app') else './tracks/bike_tracks_feature_collection.geojson'

#     # Write the FeatureCollection to a GeoJSON file
#     with open(tracks_path, 'w') as geojson_file:
#         json.dump(feature_collection, geojson_file, indent=4)
    
#     # Update the last fetch time if data fetch is successful
#     # current_time = datetime.now()
#     # update_last_fetch_time(city, current_time)

#     return {"measurements": list(MeasurementModel.objects.all().values()), "tracks": list(TrackModel.objects.all().values()), "geojson_features": feature_collection}



# async def fetch(session, url):
#     """Fetch a URL asynchronously."""
#     try:
#         async with session.get(url) as response:
#             response.raise_for_status()
#             return await response.json()
#     except aiohttp.ClientError as e:
#         print(f"Request failed for {url}: {e}")
#         return None
    
# @sync_to_async
# def delete_all_measurements(city, BoxTable, SensorTable, SensorDataTable, TracksTable):
#     # Delete records with city column
#     BoxTable.objects.filter(city=city).delete()
    
#     # Find box_ids associated with the specified city
#     box_ids = BoxTable.objects.filter(city=city).values_list('box_id', flat=True)
    
#     # Delete from tables without city column based on box_ids
#     SensorTable.objects.filter(box_id__in=box_ids).delete()
#     SensorDataTable.objects.filter(box_id__in=box_ids).delete()
#     TracksTable.objects.filter(box_id__in=box_ids).delete()


# @sync_to_async
# def update_or_create_measurement(MeasurementModel, box_id, sensor_title, name, sensor_id, sensor_icon,sensor_unit,sensor_type,sensor_value,updated_at,created_at, last_measurement_at, coordinates):

#     return MeasurementModel.objects.create(
#                 box_id=box_id,
#                 name=name,
#                 sensor_id=sensor_id,
#                 sensor_icon=sensor_icon,
#                 sensor_title=sensor_title,
#                 sensor_unit=sensor_unit,
#                 sensor_type=sensor_type,
#                 sensor_value=sensor_value,
#                 updated_at=updated_at,
#                 created_at=created_at,
#                 last_measurement_at=last_measurement_at,
#                 location=coordinates
#             )

# @sync_to_async
# def update_or_create_track_measurement(TrackMeasurementModel, box_id, sensor_id, sensor_values):
    
#     return TrackMeasurementModel.objects.create(
#         box_id=box_id,
#         sensor_id=sensor_id,
#         measurements=sensor_values
#     )

# @sync_to_async
# def update_or_create_tracks(TrackModel, box_id, timestamp, track):

#     return TrackModel.objects.create(
#         box_id=box_id,
#         timestamp=timestamp,
#         track_data=track
#     )


# async def fetch_and_store_data(city):
#     base_path = '/app/tracks' if os.path.exists('/app') else './tracks'
    
#     # BoxModel = BoxTable
#     # SensorModel = SensorTable
#     # SensordataModel = SensorDataTable
#     # Trackmodel= TracksTable

#     # Model and path assignment based on city
#     if city == "ms":
#         tracks_path = os.path.join(base_path, 'Agg_bike_tracks_ms.geojson')
#     elif city == "os":
#         tracks_path = os.path.join(base_path, 'Agg_bike_tracks_os.geojson')
#     else:
#         raise ValueError("City not supported")
    
#     # Fetch data
#     bbox = {"W": "7.85", "S": "52.19", "E": "8.17", "N": "52.37"} if city == "os" else {"W": "7.50", "S": "51.87", "E": "7.75", "N": "52.02"}
#     bbox_str = f"{bbox['W']},{bbox['S']},{bbox['E']},{bbox['N']}"
#     # params = {"grouptag": "bike", "bbox": bbox_str}
    
#     # Call delete_all_measurements for the specified city
#     await delete_all_measurements(city, BoxTable, SensorTable, SensorDataTable, TracksTable)

#     base_url = f"https://api.opensensemap.org/boxes?grouptag=bike&bbox={bbox_str}"
#     async with aiohttp.ClientSession() as session:
#         response = await fetch(session, base_url)
        
#         if response is None:
#             print("Failed to fetch the list of boxes.")
#             return
#         # breakpoint()
#         id_list = [{"id": entry["_id"], "timestamp": entry["currentLocation"]["timestamp"]} for entry in response]
        
#         feature_collection = {"type": "FeatureCollection", "features": []}

#         boxes = []
#         sensors = []
#         sensordatas = []
#         track_data= []

#         async def fetch_measurements(entry):
#             id, timestamp = entry["id"], entry["timestamp"]
            
#             # Fetch box measurements
#             # measurements = requests.get(f"https://api.opensensemap.org/boxes/{id}").json()
#             measurements_url = f"https://api.opensensemap.org/boxes/{id}"
#             measurements = await fetch(session, measurements_url)
#             if measurements is None :
#                 print(f"Failed to fetch measurements for box ID {id}")
#                 pass

            
#             box_id, name, updated_at, created_at, last_measurement_at = measurements["_id"], measurements.get("name"), measurements.get("updatedAt"), measurements.get("createdAt"), measurements.get("lastMeasurementAt")
#             coordinates = measurements.get("currentLocation", {}).get('coordinates')

#             # breakpoint()

#             box = BoxTable(
#                 box_id=box_id,
#                 name=name,
#                 city=city,
#                 created_at=created_at,
#                 updated_at=updated_at,
#                 last_measurement_at=last_measurement_at,
#                 coordinates=coordinates
#             )
#             boxes.append(box)
#             # print(boxes)
#             for sensor in measurements["sensors"]:

#                 sensor_id = sensor.get("_id")
#                 sensor_icon = sensor.get("icon")
#                 sensor_title = sensor.get("title")
#                 sensor_unit = sensor.get("unit")
#                 sensor_type = sensor.get("sensorType")
#                 last_measurement = sensor.get("lastMeasurement", {})
#                 sensor_value = last_measurement.get("value")

#                 sensor_obj = SensorTable(
#                     sensor_id=sensor_id,
#                     box_id=box,
#                     sensor_title=sensor_title,
#                     sensor_icon=sensor_icon,
#                     sensor_unit=sensor_unit,
#                     sensor_type=sensor_type,
#                     city= city,
#                     sensor_value=sensor_value
#                 )
#                 sensors.append(sensor_obj)
#                 # print(sensors)
#                 # await update_or_create_measurement(MeasurementModel, box_id, sensor_title, name, sensor_id, sensor_icon,sensor_unit,sensor_type,sensor_value,updated_at,created_at, last_measurement_at, coordinates)
#                 # breakpoint()
#                 # Fetch track measurements for each sensor
#                 # sensor_values = requests.get(f'https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?to-date={timestamp}').json()
#                 tracks_data_url = f'https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?from-date={created_at}to-date={timestamp}'
#                 sensor_values = await fetch(session, tracks_data_url)
#                 if sensor_values is None:
#                     print(f"Failed to fetch track data for sensor ID {sensor_id}")
#                     continue
                
#                 # await update_or_create_track_measurement(TrackMeasurementModel, box_id, sensor_id, sensor_values)
                
#                 sensor_data = SensorDataTable(
#                     sensor_id=sensor_obj,
#                     box_id=box,
#                     sensor_title= sensor_title,
#                     timestamp=timestamp,
#                     city= city,
#                     value=sensor_values
#                 )
#                 sensordatas.append(sensor_data)
#                 # print(sensordatas)
#             # breakpoint()
#             # Fetch and save bike tracks
#             # track = requests.get(f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&to-date={timestamp}").json()
#             route_url = f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&from-date={created_at}&to-date={timestamp}"
#             # breakpoint()
#             track = await fetch(session, route_url)
            

#             # await update_or_create_tracks(TrackModel, box_id, timestamp, track)

#             # # Prepare TrackModel instance
#             tracks= TracksTable(
#                     box_id=box_id,
#                     tracks=track,
#                     city= city,
#                     timestamp=timestamp
#                 )
#             track_data.append(tracks)
           
#             # Collect feature for GeoJSON file
#             feature = create_feature(track, id, timestamp)
#             if feature:
#                 feature_collection['features'].append(feature)

#         await asyncio.gather(*(fetch_measurements(entry) for entry in id_list))
#         print('success')
#         # breakpoint()
        

#         # Bulk create Box and Sensor instances asynchronously
#         if boxes:
#             await sync_to_async(BoxTable.objects.bulk_create)(
#                 [obj for obj in boxes if isinstance(obj, BoxTable)],
#                 ignore_conflicts=True
#             )

#         if sensors:
#             await sync_to_async(SensorTable.objects.bulk_create)(
#                 [obj for obj in sensors if isinstance(obj, SensorTable)],
#                 ignore_conflicts=True
#             )

#         # Bulk create SensorData instances asynchronously
#         if sensordatas:
#             await sync_to_async(SensorDataTable.objects.bulk_create)(
#                 sensordatas,
#                 ignore_conflicts=True
#             )

#         # Bulk create LocationData instances asynchronously
#         if track_data:
#             await sync_to_async(TracksTable.objects.bulk_create)(
#                 track_data,
#                 ignore_conflicts=True
#             )
#     # breakpoint()
    
#     # Write feature collection to GeoJSON file
#     with open(tracks_path, 'w') as geojson_file:
#         json.dump(feature_collection, geojson_file, indent=2)

#     return {"status": "Data collection successfull check the admin page for the data"}



from asgiref.sync import sync_to_async

async def fetch(session, url):
    """Fetch a URL asynchronously."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Request failed for {url}: {e}")
        return None

async def fetch_and_store_data(city):
    base_path = '/app/tracks' if os.path.exists('/app') else './tracks'

    if city == "ms":
        tracks_path = os.path.join(base_path, 'Agg_bike_tracks_ms.geojson')
        bbox = {"W": "7.50", "S": "51.87", "E": "7.75", "N": "52.02"}
    elif city == "os":
        tracks_path = os.path.join(base_path, 'Agg_bike_tracks_os.geojson')
        bbox = {"W": "7.85", "S": "52.19", "E": "8.17", "N": "52.37"}
    else:
        raise ValueError("City not supported")
    
    bbox_str = f"{bbox['W']},{bbox['S']},{bbox['E']},{bbox['N']}"
    base_url = f"https://api.opensensemap.org/boxes?grouptag=bike&bbox={bbox_str}"
    
    async with aiohttp.ClientSession() as session:
        response = await fetch(session, base_url)
        
        if response is None:
            print("Failed to fetch the list of boxes.")
            return
        
        id_list = [{"id": entry["_id"], "timestamp": entry["currentLocation"]["timestamp"]} for entry in response]

        async def fetch_measurements(entry):
            id, timestamp = entry["id"], entry["timestamp"]
            
            # Fetch box measurements
            measurements_url = f"https://api.opensensemap.org/boxes/{id}"
            measurements = await fetch(session, measurements_url)
            if measurements is None:
                print(f"Failed to fetch measurements for box ID {id}")
                return
            
            box_id = measurements["_id"]
            name = measurements.get("name")
            updated_at = measurements.get("updatedAt")
            created_at = measurements.get("createdAt")
            last_measurement_at = measurements.get("lastMeasurementAt")
            coordinates = measurements.get("currentLocation", {}).get('coordinates')

            # Update or create box
            box, created = await sync_to_async(BoxTable.objects.update_or_create)(
                box_id=box_id,
                defaults={
                    "name": name,
                    "city": city,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "last_measurement_at": last_measurement_at,
                    "coordinates": coordinates,
                },
            )
            # breakpoint()
            for sensor in measurements["sensors"]:
                sensor_id = sensor["_id"]
                sensor_icon = sensor["icon"]
                sensor_title = sensor["title"]
                sensor_unit = sensor["unit"]
                sensor_type = sensor["sensorType"]
                last_measurement = sensor.get("lastMeasurement", {})
                sensor_value = last_measurement.get("value")
                
                # Update or create sensor
                sensor_obj, created =await sync_to_async(SensorTable.objects.update_or_create)(
                    sensor_id=sensor_id,
                    defaults={
                        "box_id": box,
                        "sensor_title": sensor_title,
                        "sensor_icon": sensor_icon,
                        "sensor_unit": sensor_unit,
                        "sensor_type": sensor_type,
                        "city": city,
                        "sensor_value": sensor_value,
                    },
                )
               
                # Fetch sensor data
                tracks_data_url = f"https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?&to-date={timestamp}"
                sensor_values = await fetch(session, tracks_data_url)
                if sensor_values is None:
                    print(f"Failed to fetch data for sensor ID {sensor_id}")
                    continue
                # breakpoint()
                # Update or create sensor data
                # for data_point in sensor_values:
                await sync_to_async(SensorDataTable.objects.update_or_create)(
                    sensor_id=sensor_obj,
                    timestamp=timestamp,
                    defaults={
                        "box_id": box,
                        "city": city,
                        "value": sensor_values,
                        "sensor_title": sensor_title,
                    },
                )
            
            # Fetch and save bike tracks
            route_url = f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&from-date={created_at}&to-date={timestamp}"
            track = await fetch(session, route_url)
            if track is None:
                print(f"Failed to fetch track data for box ID {id}")
                return
            
            # Update or create track data
            await sync_to_async(TracksTable.objects.update_or_create)(
                box_id=box,
                timestamp=timestamp,
                defaults={
                    "tracks": track,
                    "city": city,
                },
            )

        await asyncio.gather(*(fetch_measurements(entry) for entry in id_list))
        print("Data successfully updated/created.")

    return {"status": "Data collection successfull check the admin page for the data"}