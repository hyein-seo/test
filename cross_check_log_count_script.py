"""
The entry point of the Python Wheel
"""

import argparse
import importlib
from typing import Dict, Optional

from pyspark.sql import SparkSession
from devplay_analytics.core.devplay_analytics.devplay_analytics_admin import DevPlayAnalyticsAdmin
from devplay_analytics.core.config.config_provider import ConfigProvider
from devplay_analytics.logging import initialize_logger
from devplay_analytics.core.env import Env


# XXX(junbong): 이 Python 스크립트는 Databricks 클러스터에서 실행되어서, data-analytics-config 를 읽어올 수 없음. 따라서 Job 실행시에
# args 로 config, config_test 를 넘겨받아 이 클래스에서 적절히 Config 를 제공해준다.
class DevplayAnalyticsConfig(ConfigProvider):
    def __init__(self, config: Dict, config_test: Dict, *args):
        initialize_logger()

        super().__init__(*args)
        self._config = config
        self._config_test = config_test

    def _load_config(self):
        return

    def _get_config_dict(self, is_test: bool):
        return self._config if not is_test else self._config_test

    def build_devplay_analytics_admin(self, target_date: Optional[str] = None) -> DevPlayAnalyticsAdmin:
        env_dict = {'game_code': self._game_code, 'env_code': self._env_code}
        if target_date is not None:
            env_dict['target_date'] = target_date
        env = Env(**env_dict)
        spark = SparkSession.builder \
            .config('spark.sql.sources.partitionColumnTypeInference.enabled', 'false') \
            .getOrCreate()
        return DevPlayAnalyticsAdmin(config=self, env=env, spark=spark)


def main(parsed_args):
    name = f'cross_check_log_count.{parsed_args.log_type}'
    job = importlib.import_module(name)

    # Build DevplayAnalyticsAdmin
    DA = DevplayAnalyticsConfig(
        eval(parsed_args.config),
        eval(parsed_args.config_test),
        parsed_args.game_code,
        parsed_args.env_code,
        False,  # is_test_read
        parsed_args.test    # is_test_write
    ).build_devplay_analytics_admin(target_date=parsed_args.target_date)

    params = {
        'log_type': parsed_args.log_type,
        'etl_json_start': parsed_args.etl_json_start,
        'is_test': parsed_args.test
    }
    job.run(DA, params)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--game_code', type=str)
    parser.add_argument('--env_code', type=str)
    parser.add_argument('--log_type', type=str)
    parser.add_argument('--target_date', type=str)
    parser.add_argument('--config', type=str)
    parser.add_argument('--config_test', type=str)
    parser.add_argument('--test', action='store_true', default=False)
    parsed_args = parser.parse_args()

    print(parsed_args)
    main(parsed_args)
