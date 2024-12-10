from django.urls import path
from . import views

# app_name = 'sensebox'
urlpatterns = [
    path('fetch-data/<str:city>/', views.fetch_bike_data, name='fetch_bike_data'),
    path('tracks/<str:city>/', views.preprocessing_tracks, name='preprocessing_tracks'),
    path('sensordata/<str:city>/', views.preprocessing_sensors, name='preprocessing_sensors'),
    path('bikeability-index/', views.bikeability, name='bikeability'),
]
