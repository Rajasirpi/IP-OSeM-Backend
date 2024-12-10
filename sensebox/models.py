from django.db import models

# 1. Box Model
class BoxTable(models.Model):
    box_id = models.CharField(max_length=100, primary_key=True)
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    last_measurement_at = models.DateTimeField(null=True, blank=True)
    city = models.CharField(max_length=100)
    coordinates = models.JSONField()

    def __str__(self):
        return self.box_id


# 2. Sensor Model
class SensorTable(models.Model):
    sensor_id = models.CharField(max_length=100, primary_key=True)
    box_id = models.ForeignKey(BoxTable, on_delete=models.CASCADE, related_name='sensors')
    sensor_title = models.CharField(max_length=255)
    sensor_icon = models.CharField(max_length=100, null=True, blank=True)
    sensor_unit = models.CharField(max_length=50)
    sensor_type = models.CharField(max_length=50)
    sensor_value = models.FloatField(null=True, blank=True)
    city = models.CharField(max_length=100, default='city')

    def __str__(self):
        return f"{self.sensor_id}"


# 3. SensorData Model (for time series data)
class SensorDataTable(models.Model):
    data_id = models.AutoField(primary_key=True)
    sensor_id = models.ForeignKey(SensorTable, on_delete=models.CASCADE, related_name='data')
    box_id = models.ForeignKey(BoxTable, on_delete=models.CASCADE, related_name='sensor_data')
    sensor_title = models.CharField(max_length=255)
    timestamp = models.DateTimeField()
    value = models.JSONField()
    city = models.CharField(max_length=100, default='city')

    class Meta:
        unique_together = ('sensor_id', 'timestamp')  # Ensures no duplicate entries for the same timestamp

    def __str__(self):
        return f"{self.sensor_id.sensor_title} data at {self.timestamp}"


# 4. LocationData Model
class TracksTable(models.Model):
    id = models.AutoField(primary_key=True)
    box = models.ForeignKey(BoxTable, on_delete=models.CASCADE, related_name='locations')
    timestamp = models.DateTimeField()
    tracks = models.JSONField()
    city = models.CharField(max_length=255)

    def __str__(self):
        return f"Location for {self.box.name} at {self.timestamp}"
