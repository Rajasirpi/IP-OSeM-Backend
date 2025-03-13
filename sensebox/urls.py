from django.urls import path
from . import views

# app_name = 'sensebox'
urlpatterns = [
    path('fetch-data/<str:city>/', views.fetch_bike_data, name='fetch_bike_data'),
    path('tracks/<str:city>/', views.preprocessing_tracks, name='preprocessing_tracks'),
    path('sensordata/<str:city>/', views.preprocessing_sensors, name='preprocessing_sensors'),
    path('bikeability-index/<str:city>/', views.bikeability, name='bikeability'),
    path('anonymization/<str:city>/', views.anonymization, name='anonymization'),
]
