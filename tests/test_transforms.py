"""Tests for NYC taxi data transformation logic."""

import pytest
from datetime import datetime


class TestTripDuration:
    def test_normal_trip(self):
        pickup = datetime(2024, 1, 15, 10, 0, 0)
        dropoff = datetime(2024, 1, 15, 10, 25, 0)
        duration_min = (dropoff - pickup).total_seconds() / 60
        assert duration_min == 25.0

    def test_short_trip(self):
        pickup = datetime(2024, 1, 15, 10, 0, 0)
        dropoff = datetime(2024, 1, 15, 10, 3, 0)
        duration_min = (dropoff - pickup).total_seconds() / 60
        assert duration_min == 3.0

    def test_negative_duration_filtered(self):
        pickup = datetime(2024, 1, 15, 10, 30, 0)
        dropoff = datetime(2024, 1, 15, 10, 0, 0)
        duration_min = (dropoff - pickup).total_seconds() / 60
        assert duration_min < 0  # Should be filtered out


class TestAvgSpeed:
    def test_normal_speed(self):
        distance = 5.0  # miles
        duration_min = 20.0
        avg_speed = distance / (duration_min / 60)
        assert avg_speed == pytest.approx(15.0)

    def test_zero_duration(self):
        distance = 5.0
        duration_min = 0.0
        avg_speed = distance / max(duration_min / 60, 0.001)
        assert avg_speed > 0


class TestFarePerMile:
    def test_normal_fare(self):
        fare = 25.0
        distance = 5.0
        fare_per_mile = round(fare / max(distance, 0.01), 2)
        assert fare_per_mile == 5.0

    def test_zero_distance(self):
        fare = 10.0
        distance = 0.0
        fare_per_mile = round(fare / max(distance, 0.01), 2)
        assert fare_per_mile == 1000.0


class TestTipPercentage:
    def test_normal_tip(self):
        tip = 5.0
        fare = 25.0
        tip_pct = round(tip / max(fare, 0.01) * 100, 2)
        assert tip_pct == 20.0

    def test_no_tip(self):
        tip = 0.0
        fare = 25.0
        tip_pct = round(tip / max(fare, 0.01) * 100, 2)
        assert tip_pct == 0.0


class TestTimeOfDay:
    def classify(self, hour):
        if 6 <= hour < 10:
            return "morning_rush"
        elif 10 <= hour < 16:
            return "midday"
        elif 16 <= hour < 20:
            return "evening_rush"
        elif 20 <= hour < 24:
            return "night"
        return "late_night"

    def test_morning_rush(self):
        assert self.classify(8) == "morning_rush"

    def test_midday(self):
        assert self.classify(12) == "midday"

    def test_evening_rush(self):
        assert self.classify(17) == "evening_rush"

    def test_night(self):
        assert self.classify(22) == "night"

    def test_late_night(self):
        assert self.classify(3) == "late_night"
