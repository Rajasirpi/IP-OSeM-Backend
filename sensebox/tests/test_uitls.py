import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from django.test import TestCase
from sensebox.utils import fetch_and_store_data


class FetchAndStoreDataTest(TestCase):
    @patch('sensebox.utils.fetch', new_callable=AsyncMock)
    @patch('sensebox.utils.delete_all_measurements', new_callable=AsyncMock)
    @patch('sensebox.utils.BoxTable.objects.bulk_create')
    @patch('sensebox.utils.SensorTable.objects.bulk_create')
    @patch('sensebox.utils.SensorDataTable.objects.bulk_create')
    @patch('sensebox.utils.TracksTable.objects.bulk_create')
    def test_fetch_and_store_data_ms(
        self,
        mock_tracks_bulk,
        mock_sensordata_bulk,
        mock_sensor_bulk,
        mock_box_bulk,
        mock_delete,
        mock_fetch
    ):
        # Mock API response for /boxes
        mock_fetch.side_effect = [
            [  # First fetch: box list
                {"_id": "box123", "currentLocation": {"timestamp": "2025-08-01T12:00:00Z"}}
            ],
            {  # Second fetch: box details
                "_id": "box123",
                "name": "Test Box",
                "updatedAt": "2025-08-01T12:00:00Z",
                "createdAt": "2025-08-01T11:00:00Z",
                "lastMeasurementAt": "2025-08-01T11:59:00Z",
                "currentLocation": {"coordinates": [7.5, 51.9]},
                "sensors": [
                    {
                        "_id": "sensor123",
                        "icon": "thermometer",
                        "title": "Temperature",
                        "unit": "°C",
                        "sensorType": "temperature",
                        "lastMeasurement": {"value": "25.5"}
                    }
                ]
            },
            [  # Third fetch: sensor data
                {"timestamp": "2025-08-01T11:30:00Z", "value": "25.5"}
            ],
            {  # Fourth fetch: track data
                "type": "FeatureCollection", "features": []
            }
        ]

        result = asyncio.run(fetch_and_store_data("ms"))

        self.assertEqual(result["status"], "Data collection successfull check the admin page for the data")
        mock_delete.assert_called_once()
        self.assertTrue(mock_box_bulk.called)
        self.assertTrue(mock_sensor_bulk.called)
        self.assertTrue(mock_sensordata_bulk.called)
        self.assertTrue(mock_tracks_bulk.called)
