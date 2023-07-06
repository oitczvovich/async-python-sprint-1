import logging
import multiprocessing

from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


logger = logging.getLogger(__name__)


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """

    manager = multiprocessing.Manager()
    fetch_data_queue = manager.Queue()

    aggregate_data_queue = manager.Queue()
    analyz_data_queue = manager.Queue()

    data_fetching_producer = DataFetchingTask(
        fetch_data_queue=fetch_data_queue,
        cities=CITIES
        )
    data_calculation_consumer = DataCalculationTask(
        fetch_data_queue=fetch_data_queue,
        aggregate_data_queue=aggregate_data_queue,
    )
    data_aggregation_consumer = DataAggregationTask(
        aggregate_data_queue=aggregate_data_queue,
        analyz_data_queue=analyz_data_queue,
    )
    data_analyzing_consumer = DataAnalyzingTask(
        analyz_data_queue=analyz_data_queue
    )

    logger.info("Начинаем анализ погодных условий.")

    data_fetching_producer.start()
    data_calculation_consumer.start()
    data_aggregation_consumer.start()
    data_analyzing_consumer.start()

    data_fetching_producer.join()
    data_calculation_consumer.join()
    data_aggregation_consumer.join()
    data_analyzing_consumer.join()

    logger.info("Анализ погодных условий завершен.")


if __name__ == "__main__":
    forecast_weather()
