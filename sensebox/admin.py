from django.contrib import admin
from .models import BoxTable, SensorTable, SensorDataTable, TracksTable, BoxTableBackup, SensorTableBackup, SensorDataTableBackup, TracksTableBackup

@admin.register(BoxTable)
class BoxTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'name', 'updated_at', 'city', 'coordinates')
    search_fields = ('box_id','name', 'city')

@admin.register(SensorTable)
class SensorTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title', 'sensor_value')
    search_fields = ('box_id__name', 'sensor_title')

@admin.register(SensorDataTable)
class SensorDataTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title')
    search_fields = ('box_id__name', 'sensor_title')

@admin.register(TracksTable)
class  TracksTableAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'timestamp', 'city')
    search_fields = ('box_id__name',)


@admin.register(BoxTableBackup)
class BoxTableBackupAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'name', 'updated_at', 'city', 'coordinates')
    search_fields = ('box_id','name', 'city')

@admin.register(SensorTableBackup)
class SensorTableBackupAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title', 'sensor_value')
    search_fields = ('box_id__name', 'sensor_title')

@admin.register(SensorDataTableBackup)
class SensorDataTableBackupAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'sensor_id', 'sensor_title')
    search_fields = ('box_id__name', 'sensor_title')

@admin.register(TracksTableBackup)
class TracksTableBackupAdmin(admin.ModelAdmin):
    list_display = ('box_id', 'timestamp', 'city')
    search_fields = ('box_id__name',)
