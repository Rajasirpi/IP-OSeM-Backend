from django.apps import AppConfig
import threading
import os

class SenseboxConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'sensebox'

    def ready(self):
        from django.core.management import call_command

        def run_fetch():
            call_command('fetch_bike_data', 'ms')
            call_command('fetch_bike_data', 'os')

        if os.environ.get('RUN_MAIN') != 'true':
            return  # Avoid duplicate execution due to runserver autoreload

        # Run in separate thread to avoid blocking startup
        threading.Thread(target=run_fetch).start()