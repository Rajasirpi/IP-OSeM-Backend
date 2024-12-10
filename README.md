# IP-OSeM-Backend

This project is a backend implementation for processing bike tracks and sensor data from the OpenSenseMap API. The system supports data retrieval, storage, and preprocessing for specific cities.

---

## Features

- Fetches and stores bike track and sensor data from the OpenSenseMap API.
- Supports data handling for multiple cities (e.g., `ms`, `os`).
- Processes data into GeoJSON format grouped by daily intervals.
- Stores data in PostgreSQL using Django ORM.
- Runs efficiently in both local and Docker environments.

---

## Prerequisites

Before starting, ensure you have the following installed on your system:

1. **Python 3.8+**
2. **Django**
3. **Docker & Docker Compose**
4. **Pipenv** (optional but recommended for managing dependencies)

---

## Setup Instructions

1. Clone the Repository
2. Set Up the virtual Environment 
3. Install Dependencies
   ```using Pip:
   pip install -r requirements.txt
   
4. Run the following commands to create and apply migrations:
   ```
   python manage.py makemigrations
   python manage.py migrate
   python manage.py runserver
   ```
5. Or using docker:
   ```
   docker-compose up --build
    ```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgements

- OpenSenseMap API
- Django Framework



