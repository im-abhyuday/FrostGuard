"""
Environmental ambient model: diurnal variation, seasonal offsets, and microclimate fluctuations.
"""

import math
from datetime import datetime
import numpy as np


class EnvironmentModel:
    """
    Computes ambient temperature and relative humidity based on geographic location,
    diurnal sinusoidal cycle (peak ~15:00, minimum ~05:00), month-based seasonal offset,
    and localized stochastic drift.
    """

    def __init__(self, rng: np.random.Generator):
        self.rng = rng
        self._temp_walk = 0.0
        self._humidity_walk = 0.0

    def compute_ambient(
        self,
        base_city_temp: float,
        timestamp: datetime,
    ) -> tuple[float, float]:
        """
        Calculates (ambient_temp_c, ambient_humidity_pct).
        """
        # 1. Diurnal cycle (period = 24h, peak at 15:00, trough at 05:00)
        # Using a 24h cycle offset so peak is at 15:00 (15h) and trough is at 03:00-05:00
        hour_frac = timestamp.hour + timestamp.minute / 60.0 + timestamp.second / 3600.0
        # Diurnal angle: 0 at 15:00 -> cos(0) = 1 (peak). At 03:00-05:00 -> cos ~ -0.9 to -1.0
        diurnal_angle = (hour_frac - 15.0) * (2.0 * math.pi / 24.0)
        diurnal_temp_swing = 7.5 * math.cos(diurnal_angle)

        # 2. Seasonal offset by month (Indian subcontinent climatology)
        month = timestamp.month
        if month in (4, 5, 6):  # Peak Summer
            seasonal_temp_offset = 6.0
            base_humidity = 35.0
        elif month in (7, 8, 9):  # Monsoon
            seasonal_temp_offset = 1.0
            base_humidity = 82.0
        elif month in (12, 1, 2):  # Winter
            seasonal_temp_offset = -6.5
            base_humidity = 55.0
        else:  # Transitional (March, Oct, Nov)
            seasonal_temp_offset = 0.5
            base_humidity = 50.0

        # 3. Stochastic random walk with mean-reversion
        self._temp_walk = 0.95 * self._temp_walk + float(self.rng.normal(0.0, 0.12))
        self._humidity_walk = 0.95 * self._humidity_walk + float(self.rng.normal(0.0, 0.4))

        ambient_temp = base_city_temp + diurnal_temp_swing + seasonal_temp_offset + self._temp_walk
        # Clamp temperature to physically observed bounds for Indian highways (-5C to 55C)
        ambient_temp = max(-5.0, min(55.0, ambient_temp))

        # Ambient humidity is inversely correlated with temperature diurnal swing
        diurnal_humidity_swing = -15.0 * math.cos(diurnal_angle)
        ambient_humidity = base_humidity + diurnal_humidity_swing + self._humidity_walk
        ambient_humidity = max(10.0, min(98.0, ambient_humidity))

        return round(ambient_temp, 2), round(ambient_humidity, 1)
