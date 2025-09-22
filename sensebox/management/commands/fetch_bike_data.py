from django.core.management.base import BaseCommand
from sensebox.utils import fetch_and_store_data
from sensebox.views import preprocessing_tracks, preprocessing_sensors, bikeability_trackwise, calculate_bikeability, expand_weights, merge_cqi
from sensebox.snapping_algorithm import process_city
from datetime import datetime
import asyncio
from asgiref.sync import async_to_sync
import time
from sqlite3 import OperationalError  # For SQLite-specific error
# If you're using another DB like Postgres, replace this with the right error type

MAX_RETRIES = 5
class Command(BaseCommand):
    help = 'Fetches bike data for a specified city'

    def add_arguments(self, parser):
        parser.add_argument('city', type=str, help="Specify the city (e.g., 'ms' or 'os')")

    def handle(self, *args, **kwargs):
        city = kwargs['city']
        # try:
        #     print((f"Fetching data for city:{city}!"))
        #     asyncio.run(fetch_and_store_data(city))
        #     timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     # print((f"[{timestamp}] Data fetched and saved successfully for city:{city}!"))
        #     self.stdout.write(f"[{timestamp}] Data fetched and saved successfully for city:{city}!")
        # except ValueError as err:
        #     self.stdout.write(str(err))
        # except Exception as err:
        #     self.stdout.write(f"An error occurred: {err}")
        
        for attempt in range(MAX_RETRIES):
            try:
                weights = {
                    "Safety": 0.4,
                    "Infrastructure_quality": 0.5,
                    "Environment_quality": 0.1
                }
                print((f"Fetching data for city:{city}!"))
                asyncio.run(fetch_and_store_data(city))
                # async_to_sync(fetch_and_store_data)(city)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                msg = f"[{timestamp}] Data fetched and saved successfully for city:{city}!"
                # print(msg)
                self.stdout.write(msg)
                time.sleep(5)
                preprocessing_tracks(city) 
                preprocessing_sensors()
                bikeability_trackwise(city)
                process_city(city) 
                time.sleep(5)
                merge_cqi(city, id_column='id', columns_to_add=None, column_rename_map=None)
                weight = expand_weights(weights)
                calculate_bikeability(city, weight)
                # osm_segements_bikeability_index_view(city, weights)
                break  # Success, exit loop

            except OperationalError as e:
                if 'database is locked' in str(e):
                    wait_time = 2
                    print(f"[Retry {attempt + 1}] DB is locked, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise

            except Exception as err:
                self.stdout.write(f"An error occurred: {err}")
                break
        