from django.urls import path
from . import views

# app_name = 'sensebox'
urlpatterns = [
    path('fetch-data/<str:city>/', views.fetch_bike_data, name='fetch_bike_data'),
    path('tracks/<str:city>/', views.preprocessing_tracks_view, name='preprocessing_tracks'),
    path('sensordata/<str:city>/', views.preprocessing_sensors_view, name='preprocessing_sensors'),
    path('bikeability_trackwise/<str:city>/', views.bikeability_trackwise_view, name='bikeability_trackwise'),
    path('bikeability-index/<str:city>/', views.bikeability, name='bikeability'),
    path('anonymization/<str:city>/', views.anonymization, name='anonymization'),
]
