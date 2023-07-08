from dataclasses import dataclass
from typing import Any


@dataclass
class InitialForecast:
    city: str  # Название города
    forecasts: list[dict[str, Any]]  # Прогноз погоды в городе


@dataclass
class DailyTemp:
    date: str  # Дата сбора прогноза погоды
    avg_temp: float | None  # Средняя дневная температура
    total_dry_hours: int  # Количество часов в день без осадков


@dataclass
class CityTemp:
    city: str  # Название города
    daily_avg_temps: list[DailyTemp]  # Средняя дневная температура по дням


class CustomException(Exception):
    pass
