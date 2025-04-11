from django.core.management.base import BaseCommand
from sensebox.utils import fetch_and_store_data
from sensebox.views import preprocessing_tracks, preprocessing_sensors, bikeability_trackwise, process_city, calculate_bikeability
from datetime import datetime
import asyncio
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

        for attempt in range(MAX_RETRIES):
            try:
                print((f"Fetching data for city:{city}!"))
                asyncio.run(fetch_and_store_data(city))
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                msg = f"[{timestamp}] Data fetched and saved successfully for city:{city}!"
                self.stdout.write(msg)
                preprocessing_tracks(city) 
                preprocessing_sensors(city)
                bikeability_trackwise(city)
                # process_city(city) 
                # calculate_bikeability(city)
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
        