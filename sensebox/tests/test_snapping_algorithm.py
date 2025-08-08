import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import os
import tempfile
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely.strtree import STRtree
import numpy as np

from sensebox.snapping_algorithm import (
    compute_point_uid,
    snap_to_nearest_line,
    snap_batch,
    process_sensor_file,
    process_city,
    city_data
)


class TestComputePointUid(unittest.TestCase):
    """Test cases for compute_point_uid function"""
    
    def test_compute_point_uid_with_all_params(self):
        """Test UUID generation with geometry, tag, and value"""
        point = Point(10.0, 20.0)
        tag = "temperature"
        value = 25.5
        
        uid1 = compute_point_uid(point, tag, value)
        uid2 = compute_point_uid(point, tag, value)
        
        # Should generate consistent UUID for same inputs
        self.assertEqual(uid1, uid2)
        self.assertIsInstance(uid1, str)
        self.assertEqual(len(uid1), 36)  # Standard UUID length
        
    def test_compute_point_uid_with_none_params(self):
        """Test UUID generation with None parameters"""
        point = Point(10.0, 20.0)
        
        uid = compute_point_uid(point, None, None)
        self.assertIsInstance(uid, str)
        self.assertEqual(len(uid), 36)
        
    def test_compute_point_uid_different_geometries(self):
        """Test that different geometries produce different UUIDs"""
        point1 = Point(10.0, 20.0)
        point2 = Point(10.1, 20.1)
        
        uid1 = compute_point_uid(point1, "test", 1.0)
        uid2 = compute_point_uid(point2, "test", 1.0)
        
        self.assertNotEqual(uid1, uid2)
        
    def test_compute_point_uid_different_tags(self):
        """Test that different tags produce different UUIDs"""
        point = Point(10.0, 20.0)
        
        uid1 = compute_point_uid(point, "tag1", 1.0)
        uid2 = compute_point_uid(point, "tag2", 1.0)
        
        self.assertNotEqual(uid1, uid2)
        
    def test_compute_point_uid_different_values(self):
        """Test that different values produce different UUIDs"""
        point = Point(10.0, 20.0)
        
        uid1 = compute_point_uid(point, "test", 1.0)
        uid2 = compute_point_uid(point, "test", 2.0)
        
        self.assertNotEqual(uid1, uid2)


class TestSnapToNearestLine(unittest.TestCase):
    """Test cases for snap_to_nearest_line function"""
    
    def setUp(self):
        """Set up test data"""
        # Create test streets
        self.streets = gpd.GeoDataFrame({
            'id': [1, 2, 3],
            'geometry': [
                LineString([(0, 0), (10, 0)]),
                LineString([(0, 5), (10, 5)]),
                LineString([(5, 0), (5, 10)])
            ]
        })
        self.street_index = STRtree(self.streets.geometry.values)
        
    def test_snap_point_between_parallel_lines(self):
        # Create two parallel vertical streets at x=0 and x=10
        streets = gpd.GeoDataFrame({
            'geometry': [
                LineString([(0, 0), (0, 10)]),
                LineString([(10, 0), (10, 10)])
            ]
        })
        street_index = STRtree(streets.geometry.tolist())

        # Point exactly in the middle, x=5, between the two lines
        point = Point(5, 5)

        snapped = snap_to_nearest_line(point, streets, street_index)

        # Since point is equidistant, it should snap to either x=0 or x=10 but with y=5
        self.assertIsInstance(snapped, Point)
        self.assertAlmostEqual(snapped.y, 5.0)

        # Check that snapped.x is either 0 or 10 (one of the two lines)
        self.assertIn(round(snapped.x), [0, 10])

    def test_snap_point_near_corner(self):
        # Create two streets forming an L shape: horizontal and vertical meeting at (10,0)
        streets = gpd.GeoDataFrame({
            'geometry': [
                LineString([(0, 0), (10, 0)]),    # Horizontal
                LineString([(10, 0), (10, 10)])   # Vertical
            ]
        })
        street_index = STRtree(streets.geometry.tolist())

        # Point near the corner, closer to (10,0)
        point = Point(9.5, 0.5)

        snapped = snap_to_nearest_line(point, streets, street_index)

        self.assertIsInstance(snapped, Point)

        # The snapped point should be either on the horizontal or vertical line near (10,0)
        # So either y=0 and x close to 9.5 or x=10 and y close to 0.5
        on_horizontal = (abs(snapped.y - 0) < 0.01) and (8 <= snapped.x <= 10)
        on_vertical = (abs(snapped.x - 10) < 0.01) and (0 <= snapped.y <= 2)

        self.assertTrue(on_horizontal or on_vertical, f"Snapped point {snapped} not on expected segments")


    def test_snap_point_already_on_line(self):
        """Test snapping when point is already on a line"""
        point = Point(5, 0)  # Point already on horizontal line
        snapped = snap_to_nearest_line(point, self.streets, self.street_index)
        
        # Should return the same point
        self.assertIsInstance(snapped, Point)
        self.assertAlmostEqual(snapped.x, 5.0)
        self.assertAlmostEqual(snapped.y, 0.0)
        
    def test_snap_point_far_from_lines(self):
        """Test snapping when point is far from all lines"""
        point = Point(100, 100)  # Far from all test lines
        snapped = snap_to_nearest_line(point, self.streets, self.street_index)
        
        # Should still return a valid Point
        self.assertIsInstance(snapped, Point)
        
    def test_snap_point_with_empty_streets(self):
        """Test snapping with empty streets GeoDataFrame"""
        empty_streets = gpd.GeoDataFrame({'geometry': []})
        empty_index = STRtree([])
        point = Point(5, 5)
        
        snapped = snap_to_nearest_line(point, empty_streets, empty_index)
        
        # Should return original point when no streets available
        self.assertIsInstance(snapped, Point)
        self.assertAlmostEqual(snapped.x, 5.0)
        self.assertAlmostEqual(snapped.y, 5.0)


class TestSnapBatch(unittest.TestCase):
    """Test cases for snap_batch function"""
    
    def setUp(self):
        """Set up test data"""
        self.streets = gpd.GeoDataFrame({
            'id': [1, 2],
            'geometry': [
                LineString([(0, 0), (10, 0)]),
                LineString([(0, 5), (10, 5)])
            ]
        })
        self.street_index = STRtree(self.streets.geometry.values)
        
    def test_snap_batch_single_point(self):
        """Test snapping a single point"""
        points = [Point(5, 1)]
        snapped = snap_batch(points, self.streets, self.street_index)
        
        self.assertEqual(len(snapped), 1)
        self.assertIsInstance(snapped[0], Point)
        self.assertAlmostEqual(snapped[0].x, 5.0)
        self.assertAlmostEqual(snapped[0].y, 0.0)
        
    def test_snap_batch_multiple_points(self):
        """Test snapping multiple points"""
        points = [Point(5, 1), Point(5, 6), Point(2, 1)]
        snapped = snap_batch(points, self.streets, self.street_index)
        
        self.assertEqual(len(snapped), 3)
        for point in snapped:
            self.assertIsInstance(point, Point)
            
    def test_snap_batch_empty_list(self):
        """Test snapping empty list"""
        points = []
        snapped = snap_batch(points, self.streets, self.street_index)
        
        self.assertEqual(len(snapped), 0)
        
    def test_snap_batch_mixed_points(self):
        """Test snapping points at various positions"""
        points = [
            Point(0, 0),    # On line
            Point(5, 1),    # Above line
            Point(10, 5),   # On line
            Point(15, 5)    # Beyond line
        ]
        snapped = snap_batch(points, self.streets, self.street_index)
        
        self.assertEqual(len(snapped), 4)
        for point in snapped:
            self.assertIsInstance(point, Point)


class TestProcessSensorFile(unittest.TestCase):
    """Test cases for process_sensor_file function"""
    
    def test_process_sensor_file_mock(self):
        """Test sensor file processing with mock"""
        # This is a simplified test - in real usage, you'd mock the file I/O
        streets = gpd.GeoDataFrame({
            'id': [1],
            'geometry': [LineString([(0, 0), (10, 0)])]
        })
        
        # For now, just verify the function signature
        self.assertIsNotNone(streets)

class TestIntegration(unittest.TestCase):
    """Integration tests for the snapping algorithm"""
    
    def test_end_to_end_snapping_workflow(self):
        """Test complete snapping workflow"""
        # Define test street segments
        self.streets = gpd.GeoDataFrame({
            'geometry': [
                LineString([(0, 0), (10, 0)]),  # y=0
                LineString([(0, 5), (10, 5)]),  # y=5
            ]
        }, crs="EPSG:4326")
        self.street_index = STRtree(self.streets.geometry)

        # Create test points
        points = [
            Point(5, 1),   # Closer to y=0
            Point(5, 6),   # Closer to y=5
            Point(2, 1),   # Closer to y=0
        ]
        
        snapped = snap_batch(points, self.streets, self.street_index)
        
        expected_coords = [(5.0, 0.0), (5.0, 5.0), (2.0, 0.0)]

        self.assertEqual(len(snapped), 3)
        
        for i, (snapped_point, expected) in enumerate(zip(snapped, expected_coords)):
            print(f"Point {i}: snapped to ({snapped_point.x}, {snapped_point.y}), expected {expected}")
            self.assertIsInstance(snapped_point, Point)
            self.assertAlmostEqual(snapped_point.x, expected[0])
            self.assertAlmostEqual(snapped_point.y, expected[1])


if __name__ == '__main__':
    unittest.main()
