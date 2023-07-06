import csv
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from multiprocessing import Pool, Process, Queue
from multiprocessing.process import AuthenticationString
from typing import Any

from external.client import YandexWeatherAPI
from entities import DailyTemp, CityTemp, InitialForecast
from utils import DRY_WEATHER, FILE_NAME, FROM_HOUR, TO_HOUR


logger = logging.getLogger(__name__)


class DataFetchingTask(Process):
    """
    Получение данных через API.
    """

    def __init__(
            self,
            *,
            fetch_data_queue: Queue,
            cities: dict[str, Any]
            ) -> None:
        super().__init__()
        self.api = YandexWeatherAPI()
        self.fetch_data_queue = fetch_data_queue
        self.cities = cities

    def get_city_forecasts(self, city_name: str) -> tuple[str, dict[str, Any]]:
        try:
            result = self.api.get_forecasting(city_name=city_name)
        except Exception as error:
            logging.exception(f"Failed to fetch data from api: {error}")
            raise error
        return city_name, result

    def run(self) -> None:
        with ThreadPoolExecutor() as pool:
            for city_forecast in pool.map(
                    self.get_city_forecasts,
                    self.cities.keys()
                    ):
                self.fetch_data_queue.put(
                    InitialForecast(
                        city=city_forecast[0],
                        forecasts=city_forecast[1]["forecasts"],
                    )
                )
            self.fetch_data_queue.put(None)


class DataCalculationTask(Process):

    def __init__(
            self,
            fetch_data_queue: Queue,
            aggregate_data_queue: Queue
            ) -> None:
        super().__init__()
        self.fetch_data_queue = fetch_data_queue
        self.aggregate_data_queue = aggregate_data_queue

    def __getstate__(self):
        """called when pickling - this hack allows subprocesses to
           be spawned without the AuthenticationString raising an error"""
        state = self.__dict__.copy()
        conf = state["_config"]
        if "authkey" in conf:
            conf["authkey"] = bytes(conf["authkey"])
        return state

    def __setstate__(self, state):
        """for unpickling"""
        state["_config"]["authkey"] = AuthenticationString(
            state["_config"]["authkey"]
            )
        self.__dict__.update(state)

    @staticmethod
    def _check_hour(hour) -> bool:
        return bool(int(hour) >= FROM_HOUR and int(hour) <= TO_HOUR)

    def get_daily_avg_temp(
            self,
            daily_forecast
            ) -> float | None:
        hours = daily_forecast["hours"]
        temps = [
            hour["temp"] for hour in hours if self._check_hour(
                hour=hour["hour"]
                )
        ]
        return round((sum(temps) / len(temps))) if temps else None

    def get_summ_dry_hours(self, daily_forecast: dict[str, Any]) -> int:
        hours = daily_forecast["hours"]
        dry_hours = 0
        for hour in hours:
            if hour["condition"] in DRY_WEATHER and self._check_hour(
                hour=hour["hour"]
            ):
                dry_hours += 1

        return dry_hours

    def calc_daily_temp(
            self,
            daily_forcecast_data: dict[str, Any]
            ) -> DailyTemp:
        daily_avg_temp = self.get_daily_avg_temp(daily_forcecast_data)
        total_dry_hours = self.get_summ_dry_hours(daily_forcecast_data)
        return (
            DailyTemp(
                date=daily_forcecast_data["date"],
                avg_temp=daily_avg_temp,
                total_dry_hours=total_dry_hours,
            )
        )

    def run(self) -> None:
        while city_forcecast_data := self.fetch_data_queue.get():
            with Pool() as pool:
                daily_avg_temp = list(
                    pool.map(
                        self.calc_daily_temp,
                        city_forcecast_data.forecasts
                    )
                )
                city_temp: CityTemp = CityTemp(
                    city=city_forcecast_data.city,
                    daily_avg_temps=daily_avg_temp,
                )
                self.aggregate_data_queue.put(city_temp)
        self.aggregate_data_queue.put(None)


class DataAggregationTask(Process):

    def __init__(
        self,
        aggregate_data_queue: Queue,
        analyz_data_queue: Queue,
    ) -> None:
        super().__init__()
        self.aggregate_data_queue = aggregate_data_queue
        self.analyz_data_queue = analyz_data_queue
    
    def agregate_forcecast(self, forcecast_data: CityTemp) -> dict[str, Any]:
        day_forecasts = {}
        avg_temp_list = []
        avg_dry_hours_list = []
        day_forecasts["Город"] = forcecast_data.city
        day_forecasts[""] = "Температура, среднее / Без осадков, часов"
        for item in forcecast_data.daily_avg_temps:
            date = item.date
            date_avg_temp = item.avg_temp
            date_total_dry_hours = item.total_dry_hours
            day_forecasts[date] = f"{date_avg_temp} / {date_total_dry_hours}"

            if date_avg_temp is not None:
                avg_temp_list.append(date_avg_temp)
                avg_dry_hours_list.append(date_total_dry_hours)

        avg_temp = round(
            reduce(lambda a, b: a + b, avg_temp_list)
            / len(avg_temp_list),
            1
        )
        avg_dry_hours = round(
            reduce(lambda a, b: a + b, avg_dry_hours_list)
            / len(avg_dry_hours_list)
        )
        day_forecasts["Среднее"] = f'{avg_temp}/{avg_dry_hours}'
        return day_forecasts

    def run(self):
        day_forecasts_list = []
        while city_forcecast_aggr_data := self.aggregate_data_queue.get():
            day_forecasts_list.append(
                self.agregate_forcecast(
                    forcecast_data=city_forcecast_aggr_data,
                )
            )

        with open(FILE_NAME, 'w') as file:
            writer = csv.DictWriter(
                file,
                delimiter=';',
                fieldnames=[*day_forecasts_list[0]]
            )
            writer.writeheader()
            writer.writerows(day_forecasts_list)

        self.analyz_data_queue.put(FILE_NAME)


class DataAnalyzingTask(Process):

    def __init__(self, analyz_data_queue: Queue):
        super().__init__()
        self.analyz_data_queue = analyz_data_queue

    def get_rating(self, row: dict[str, Any]) -> int:
        params = [float(i) for i in row["Среднее"].split("/")]
        return round(params[0] * params[1])

    def run(self) -> None:
        df_list = []
        raitings = []
        if aggregated_data_file_name := self.analyz_data_queue.get():
            with open(aggregated_data_file_name, "r") as file:
                reader = csv.DictReader(file, delimiter=";")
                for row in reader:
                    raiting = self.get_rating(row)
                    df_list.append(row | {"Рейтинг": raiting})
                    raitings.append(raiting)

            with open(aggregated_data_file_name, "w") as file:
                writer = csv.DictWriter(
                    file,
                    delimiter=";",
                    fieldnames=[*df_list[0]]
                )
                writer.writeheader()
                writer.writerows(df_list)

            most_comfort_cities = [
                df["Город"] for df in df_list if df["Рейтинг"] == max(raitings)
            ]
            if len(most_comfort_cities) == 1:
                msg = f"Самый комфортный город - {most_comfort_cities[0]}"
            else:
                msg = f"Самые комфортные города - "
                f"{', '.join(most_comfort_cities)}"
            logger.info(msg)