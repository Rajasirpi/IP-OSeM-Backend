from django.test import TestCase
from django.db.utils import IntegrityError
from sensebox.models import BoxTable, SensorTable, SensorDataTable
from django.utils import timezone

class SensorDataTableTest(TestCase):
    def setUp(self):
        self.box = BoxTable.objects.create(
            box_id="box123",
            name="Test Box",
            created_at=timezone.now(),
            updated_at=timezone.now(),
            city="TestCity",
            coordinates={"lat": 0, "lng": 0}
        )
        self.sensor = SensorTable.objects.create(
            sensor_id="sensor123",
            box_id=self.box,
            sensor_title="Temperature",
            sensor_unit="°C",
            sensor_type="temperature",
            city="TestCity"
        )

    def test_unique_sensor_timestamp(self):
        timestamp = timezone.now()
        # First entry should pass
        SensorDataTable.objects.create(
            sensor_id=self.sensor,
            box_id=self.box,
            sensor_title="Temperature",
            timestamp=timestamp,
            value={"value": 25.5},
            city="TestCity"
        )
        # Second entry with same sensor and timestamp should raise error
        with self.assertRaises(IntegrityError):
            SensorDataTable.objects.create(
                sensor_id=self.sensor,
                box_id=self.box,
                sensor_title="Temperature",
                timestamp=timestamp,
                value={"value": 26.1},
                city="TestCity"
            )
