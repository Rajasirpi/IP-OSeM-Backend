import unittest
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from unittest.mock import patch, MagicMock
import json
from shapely.geometry import LineString
from sensebox.views import split_linestring_by_day, normalize_semantic, normalization_config, calculate_bikeability, expand_weights

class TestSplitLineString(unittest.TestCase):
    def test_single_day_split(self):
        features = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0, 0], [1, 1], [2, 2]]
            },
            "properties": {
                "timestamps": [
                    "2025-08-01T10:00:00Z",
                    "2025-08-01T10:01:00Z",
                    "2025-08-01T10:02:00Z"
                ]
            }
        }
        result = split_linestring_by_day(features, "testbox1")
        self.assertEqual(len(result["features"]), 1)
        self.assertEqual(result["features"][0]["properties"]["date"], "2025-08-01")

    def test_split_due_to_date_change(self):
        features = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0, 0], [1, 1], [2, 2]]
            },
            "properties": {
                "timestamps": [
                    "2025-08-01T23:59:00Z",
                    "2025-08-02T00:01:00Z",
                    "2025-08-02T00:03:00Z"
                ]
            }
        }
        result = split_linestring_by_day(features, "testbox2")
        self.assertEqual(len(result["features"]), 1)  # Only one segment has 3 points
        self.assertEqual(result["features"][0]["properties"]["date"], "2025-08-02")

class TestNormalizeSemantic(unittest.TestCase):
    def test_linear_benefit_normalization(self):
        s = pd.Series([100, 150, 200])
        result = normalize_semantic(s, "Overtaking_Distance", normalization_config)
        self.assertAlmostEqual(result.iloc[0], 0.0)
        self.assertAlmostEqual(result.iloc[2], 1.0)

    def test_linear_cost_normalization(self):
        s = pd.Series([0, 12.5, 25])
        result = normalize_semantic(s, "Finedust_PM1", normalization_config)
        self.assertAlmostEqual(result.iloc[0], 1.0)
        self.assertAlmostEqual(result.iloc[2], 0.0)

    def test_triangular_normalization(self):
        s = pd.Series([10, 22, 30])
        result = normalize_semantic(s, "Temperature", normalization_config)
        self.assertAlmostEqual(result.iloc[0], 0.0)
        self.assertAlmostEqual(result.iloc[1], 1.0)
        self.assertAlmostEqual(result.iloc[2], 0.0)

# class TestCalculateBikeability(unittest.TestCase):
#     @patch("sensebox.views.gpd.GeoDataFrame.to_file")
#     @patch("sensebox.views.gpd.read_file")
#     @patch("sensebox.views.os.remove")
#     def test_calculate_bikeability(self, mock_read_file, mock_to_file, mock_remove):
#         from sensebox.views import calculate_bikeability
#         # Create sample GeoDataFrame
#         features = [
#                 {
#                     "type": "Feature",
#                     "properties": {
#                         "id": "way/4064462",
#                         "avg_ms_Finedust_PM1": 6.44,
#                         "avg_ms_Overtaking_Distance": 16.0,
#                         "avg_ms_Rel__Humidity": 13.98,
#                         "avg_ms_Temperature": 19.13,
#                         "avg_ms_Speed": 5.56,
#                         "sum_ms_accidents": 0.65,
#                         "avg_ms_cqi_index": 31.0,
#                         "stress_level": 4.0
#                     },
#                     "geometry": {
#                         "type": "LineString",
#                         "coordinates": [
#                             [ -1625013.657, 6235358.825 ],
#                             [ -1624994.640, 6235350.770 ],
#                             [ -1624986.239, 6235347.764 ]
#                         ]
#                     }
#                 },
#                 {
#                     "type": "Feature",
#                     "properties": {
#                         "id": "way/4064463",
#                         "avg_ms_Finedust_PM1": 8.12,
#                         "avg_ms_Overtaking_Distance": 12.0,
#                         "avg_ms_Rel__Humidity": 18.0,
#                         "avg_ms_Temperature": 22.0,
#                         "avg_ms_Speed": 6.1,
#                         "sum_ms_accidents": 1.1,
#                         "avg_ms_cqi_index": 39.0,
#                         "stress_level": 2.0
#                     },
#                     "geometry": {
#                         "type": "LineString",
#                         "coordinates": [
#                             [ -1624913.657, 6235358.825 ],
#                             [ -1624894.640, 6235350.770 ],
#                             [ -1624886.239, 6235347.764 ]
#                         ]
#                     }
#                 }
#             ]
        
#         gdf = gpd.GeoDataFrame.from_features(features)
#         gdf.set_crs(epsg=32637, inplace=True)
#         # Prevent actual file deletion
#         mock_remove.return_value = None
#         mock_read_file.return_value = gdf  # Patch read_file to return mock gdf
        

#         weights = {
#             "safety": 0.4,  
#             "infrastructure_quality": 0.5,  
#             "environment_quality": 0.1 
#         }
#         weight = expand_weights(weights)
#         response = calculate_bikeability(city="ms", weights=weight)
#         mock_to_file.return_value = None
#         self.assertEqual(response.status_code, 200)
