from django.contrib import admin
from .models import BoxTable, SensorTable, SensorDataTable, TracksTable

@admin.register(BoxTable)
class BoxTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'name', 'updated_at', 'city', 'coordinates')
    search_fields = ('box_id', 'city')

@admin.register(SensorTable)
class SensorTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title', 'sensor_value')
    search_fields = ('box_id', 'sensor_title')

@admin.register(SensorDataTable)
class SensorDataTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title')
    search_fields = ('box_id',)

@admin.register(TracksTable)
class  TracksTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'timestamp', 'city')
    search_fields = ('box_id',)

