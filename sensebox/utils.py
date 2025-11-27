import requests
from sensebox.models import BoxTable, SensorTable, SensorDataTable, TracksTable, BoxTableBackup, SensorTableBackup, SensorDataTableBackup, TracksTableBackup
import json
import os
from django.db.models import Count
from django.db import transaction
from django.db import connection 
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


async def fetch(session, url):
    """Fetch a URL asynchronously."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Request failed for {url}: {e}")
        return None

@sync_to_async
def backup_and_delete_measurements(city, BoxTable, SensorTable, SensorDataTable, TracksTable,
                                   BoxTableBackup, SensorTableBackup, SensorDataTableBackup, TracksTableBackup):
    # Get box_ids to backup
    box_ids = BoxTable.objects.filter(city=city).values_list('box_id', flat=True)

    # inorder to not overload with backups
    BoxTableBackup.objects.filter(city=city).delete()
    SensorTableBackup.objects.filter(box_id__in=box_ids).delete()
    SensorDataTableBackup.objects.filter(box_id__in=box_ids).delete()
    TracksTableBackup.objects.filter(box_id__in=box_ids).delete()

    # Backup boxes
    for box in BoxTable.objects.filter(city=city):
        BoxTableBackup.objects.create(
            box_id=box.box_id,
            name=box.name,
            city=box.city,
            created_at=box.created_at,
            updated_at=box.updated_at,
            last_measurement_at=box.last_measurement_at,
            coordinates=box.coordinates,
        )
    # breakpoint()
    # Backup sensors
    for sensor in SensorTable.objects.filter(box_id__in=box_ids):
        SensorTableBackup.objects.create(
            sensor_id=sensor.sensor_id,
            box_id=sensor.box_id.box_id,   # 🔑 store ID, not FK object
            sensor_title=sensor.sensor_title,
            sensor_icon=sensor.sensor_icon,
            sensor_unit=sensor.sensor_unit,
            sensor_type=sensor.sensor_type,
            sensor_value=sensor.sensor_value,
            city=sensor.city,
        )

    # Backup sensor data
    for data in SensorDataTable.objects.filter(box_id__in=box_ids):
        SensorDataTableBackup.objects.create(
            sensor_id=data.sensor_id.sensor_id,  # 🔑 store ID
            box_id=data.box_id.box_id,           # 🔑 store ID
            sensor_title=data.sensor_title,
            timestamp=data.timestamp,
            value=data.value,
            city=data.city,
        )

    # Backup tracks
    for track in TracksTable.objects.filter(box__box_id__in=box_ids):
        TracksTableBackup.objects.create(
            box_id=track.box.box_id,   # 🔑 use FK → box.box_id
            timestamp=track.timestamp,
            tracks=track.tracks,
            city=track.city,
        )

    # Delete originals
    BoxTable.objects.filter(city=city).delete()
    SensorTable.objects.filter(box_id__in=box_ids).delete()
    SensorDataTable.objects.filter(box_id__in=box_ids).delete()
    TracksTable.objects.filter(box_id__in=box_ids).delete()

async def fetch_and_store_data(city):
    base_path = '/app/tracks' if os.path.exists('/app') else './tracks'

    # Model and path assignment based on city
    if city == "ms":
        tracks_path = os.path.join(base_path, 'Agg_bike_tracks_ms.geojson')
    elif city == "os":
        tracks_path = os.path.join(base_path, 'Agg_bike_tracks_os.geojson')
    else:
        raise ValueError("City not supported")
    
    # Fetch data
    bbox = {"W": "7.85", "S": "52.19", "E": "8.17", "N": "52.37"} if city == "os" else {"W": "7.50", "S": "51.87", "E": "7.75", "N": "52.02"}
    bbox_str = f"{bbox['W']},{bbox['S']},{bbox['E']},{bbox['N']}"
    # params = {"grouptag": "bike", "bbox": bbox_str}
    
    # Call delete_all_measurements for the specified city
    # await delete_all_measurements(city, BoxTable, SensorTable, SensorDataTable, TracksTable)
    await backup_and_delete_measurements(city, BoxTable, SensorTable, SensorDataTable, TracksTable, BoxTableBackup, SensorTableBackup, SensorDataTableBackup, TracksTableBackup)

    base_url = f"https://api.opensensemap.org/boxes?grouptag=bike&bbox={bbox_str}"
    async with aiohttp.ClientSession() as session:
        response = await fetch(session, base_url)
        
        if response is None:
            print("Failed to fetch the list of boxes.")
            return
        
        id_list = [{"id": entry["_id"], "timestamp": entry["currentLocation"]["timestamp"]} for entry in response]
        
        feature_collection = {"type": "FeatureCollection", "features": []}

        boxes = []
        sensors = []
        sensordatas = []
        track_data= []

        async def fetch_measurements(entry):
            id, timestamp = entry["id"], entry["timestamp"]
            
            # Fetch box measurements
            # measurements = requests.get(f"https://api.opensensemap.org/boxes/{id}").json()
            measurements_url = f"https://api.opensensemap.org/boxes/{id}"
            measurements = await fetch(session, measurements_url)
            if measurements is None :
                print(f"Failed to fetch measurements for box ID {id}")
                pass

            
            box_id, name, updated_at, created_at, last_measurement_at = measurements["_id"], measurements.get("name"), measurements.get("updatedAt"), measurements.get("createdAt"), measurements.get("lastMeasurementAt")
            coordinates = measurements.get("currentLocation", {}).get('coordinates')


            box = BoxTable(
                box_id=box_id,
                name=name,
                city=city,
                created_at=created_at,
                updated_at=updated_at,
                last_measurement_at=last_measurement_at,
                coordinates=coordinates
            )
            boxes.append(box)
            # print(boxes)
            for sensor in measurements["sensors"]:

                sensor_id = sensor.get("_id")
                sensor_icon = sensor.get("icon")
                sensor_title = sensor.get("title")
                sensor_unit = sensor.get("unit")
                sensor_type = sensor.get("sensorType")
                last_measurement = sensor.get("lastMeasurement", {})
                sensor_value = last_measurement.get("value")

                sensor_obj = SensorTable(
                    sensor_id=sensor_id,
                    box_id=box,
                    sensor_title=sensor_title,
                    sensor_icon=sensor_icon,
                    sensor_unit=sensor_unit,
                    sensor_type=sensor_type,
                    city= city,
                    sensor_value=sensor_value
                )
                sensors.append(sensor_obj)
                # print(sensors)
                # await update_or_create_measurement(MeasurementModel, box_id, sensor_title, name, sensor_id, sensor_icon,sensor_unit,sensor_type,sensor_value,updated_at,created_at, last_measurement_at, coordinates)
                
                # Fetch track measurements for each sensor
                # sensor_values = requests.get(f'https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?to-date={timestamp}').json()
                tracks_data_url = f'https://api.opensensemap.org/boxes/{id}/data/{sensor_id}/?from-date={created_at}to-date={timestamp}'
                sensor_values = await fetch(session, tracks_data_url)
                if sensor_values is None:
                    print(f"Failed to fetch track data for sensor ID {sensor_id}")
                    continue
                
                # await update_or_create_track_measurement(TrackMeasurementModel, box_id, sensor_id, sensor_values)
                
                sensor_data = SensorDataTable(
                    sensor_id=sensor_obj,
                    box_id=box,
                    sensor_title= sensor_title,
                    timestamp=timestamp,
                    city= city,
                    value=sensor_values
                )
                sensordatas.append(sensor_data)
                # print(sensordatas)
          
            # Fetch and save bike tracks
            # track = requests.get(f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&to-date={timestamp}").json()
            route_url = f"https://api.opensensemap.org/boxes/{id}/locations?format=geojson&from-date={created_at}&to-date={timestamp}"
            
            track = await fetch(session, route_url)
            

            # await update_or_create_tracks(TrackModel, box_id, timestamp, track)

            # # Prepare TrackModel instance
            tracks= TracksTable(
                    box_id=box_id,
                    tracks=track,
                    city= city,
                    timestamp=timestamp
                )
            track_data.append(tracks)
           
            # Collect feature for GeoJSON file
            feature = create_feature(track, id, timestamp)
            if feature:
                feature_collection['features'].append(feature)

        await asyncio.gather(*(fetch_measurements(entry) for entry in id_list))
        print('Measurement fetch complete.')
        
        # Bulk create Box and Sensor instances asynchronously
        if boxes:
            await sync_to_async(BoxTable.objects.bulk_create)(
                [obj for obj in boxes if isinstance(obj, BoxTable)],
                ignore_conflicts=True
            )

        if sensors:
            await sync_to_async(SensorTable.objects.bulk_create)(
                [obj for obj in sensors if isinstance(obj, SensorTable)],
                ignore_conflicts=True
            )

        # Bulk create SensorData instances asynchronously
        if sensordatas:
            await sync_to_async(SensorDataTable.objects.bulk_create)(
                sensordatas,
                ignore_conflicts=True
            )

        # Bulk create LocationData instances asynchronously
        if track_data:
            await sync_to_async(TracksTable.objects.bulk_create)(
                track_data,
                ignore_conflicts=True
            )
        # Allow DB to "cool down" before further operations
        # await asyncio.sleep(10)
        # connection.close()

    # # Write feature collection to GeoJSON file
    # with open(tracks_path, 'w') as geojson_file:
    #     json.dump(feature_collection, geojson_file, indent=2)

    return {"status": "Data collection successfull check the admin page for the data"}
