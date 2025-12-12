#!/bin/bash
set -e  # exit on error
set -x  # echo each command

echo ">>> Applying migrations..."
python3 -u manage.py makemigrations
python3 -u manage.py migrate

echo ">>> Starting cron..."
service cron start

echo ">>> Starting Django server..."
exec python3 -u manage.py runserver 0.0.0.0:8000

# echo ">>> Running one-time initial fetch"
# python3 -u manage.py fetch_bike_data ms
# python3 -u manage.py fetch_bike_data os

# echo ">>> Applying migrations..."
# python3 -u manage.py makemigrations
# python3 -u manage.py migrate

# echo ">>> Starting cron..."
# service cron start

# echo ">>> Starting Django server..."
# exec python3 -u manage.py runserver 0.0.0.0:8000
