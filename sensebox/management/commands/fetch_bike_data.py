from django.core.management.base import BaseCommand
from sensebox.utils import fetch_and_store_data
from datetime import datetime
import asyncio

class Command(BaseCommand):
    help = 'Fetches bike data for a specified city'

    def add_arguments(self, parser):
        parser.add_argument('city', type=str, help="Specify the city (e.g., 'ms' or 'os')")

    def handle(self, *args, **kwargs):
        city = kwargs['city']
        try:
            asyncio.run(fetch_and_store_data(city))
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.stdout.write(f"[{timestamp}] Data fetched and saved successfully for city:{city}!")
        except ValueError as err:
            self.stdout.write(str(err))
        except Exception as err:
            self.stdout.write(f"An error occurred: {err}")