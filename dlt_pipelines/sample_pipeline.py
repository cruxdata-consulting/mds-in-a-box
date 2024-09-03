
import csv
import dlt
import logging
import logging.config
import os
import time

from distutils.util import strtobool
from dotenv import load_dotenv
from itertools import islice
from typing import Any, Iterator

from dlt.common.typing import TDataItems
from dlt.pipeline.helpers import retry_load
from dlt.sources.helpers import requests

from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from dbt_model_generator import DbtModelGenerator


@dlt.source()
def demographics(limit: int = None) -> Any:
    """
    Loads CSV data manually downloaded from a range of providers. 
    
    @TODO This should be refactored to pull directly from the web download links, but total filesize is >>3GB so we pull from local for MVP
    """
    
    def _load_from_csv(path: str, label: str) -> Any:
        chunk_size = 1000

        with open(path) as f:
            row_count = sum(1 for _ in f) - 1 # Adjust for the header
            f.seek(0) # reset file pointer to start of stream
            lines = csv.DictReader(f)
            while chunk := list(islice(lines, chunk_size)):
                pipeline.collector.update('Resources', inc=len(chunk), total=row_count, label=label)
                yield chunk

    @dlt.resource(write_disposition="replace")
    def gdp() -> Iterator[TDataItems]:
        """Table contains World Bank GDP PPP from https://data.worldbank.org/indicator/NY.GDP.MKTP.PP.CD"""
        yield from _load_from_csv('data/demographics/clean/WDI_GDPPurchasingPowerParity.csv', 'gdp')
    
    @dlt.resource(write_disposition="replace")
    def population() -> Iterator[TDataItems]:
        """Table contains population breakdowns from the United Nations https://population.un.org/wpp/Download/Standard/Population/"""
        yield from _load_from_csv('data/demographics/clean/WPP2024_demographicIndicators.csv', 'population')

    return [
        gdp.add_limit(limit), 
        population.add_limit(limit)
    ]

def load_data_with_retry(pipeline, data, dataset_name, retries=1):
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(retries),
            wait=wait_exponential(multiplier=1.5, min=4, max=10),
            retry=retry_if_exception(retry_load(())),
            reraise=True,
        ):
            with attempt:
                log.info(f"Running the pipeline, attempt={attempt.retry_state.attempt_number}")
                load_info = pipeline.run(data, dataset_name=dataset_name)
                log.info(str(load_info))

                # raise on failed jobs
                load_info.raise_on_failed_jobs()
    except Exception:
        # we get here after all the failed retries
        log.suppress_and_warn("Something went wrong")
        raise

    # we get here after a successful attempt
    # see when load was started
    log.info(f"Pipeline was started: {load_info.started_at}")
    # print the information on the first load package and all jobs inside
    log.info(f"First load package info: {load_info.load_packages[0]}")
    # print the information on the first completed job in first load package
    log.info(f"First completed job info: {load_info.load_packages[0].jobs['completed_jobs'][0]}")

    # check for schema updates:
    schema_updates = [p.schema_update for p in load_info.load_packages]
    # send notifications if there are schema updates
    if schema_updates:
        # send notification
        log.info("Schema was updated!")

    dbt = DbtModelGenerator(
        dbt_staging_dir=os.environ.get('DBT_STAGING_DIR'), 
        source_name=pipeline.dataset_name, 
        config_file=f"./{os.environ.get('DLT_PROJECT_DIR')}/config.yml"
    )
    dbt.generate_models(load_info=load_info)

    return load_info

if __name__ == "__main__":
    load_dotenv('../.env')

    pipeline = dlt.pipeline(
        pipeline_name=os.environ.get('DATABASE_NAME'),
        destination=os.environ.get('DATABASE_DIALECT'),
        export_schema_path=os.environ.get('DLT_SCHEMA_DIR'),
        dev_mode=bool(strtobool(os.environ.get('DLT_DEV_MODE', 'False'))),
        progress="enlighten"
    )

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    logging.config.fileConfig(os.path.join(ROOT_DIR,"logging_config.ini"))
    log = logging.getLogger('mds')

    try:
        limit = int(os.environ.get('DLT_RECORD_LIMIT'))
    except:
        limit = None

    load_info = load_data_with_retry(
        pipeline, 
        demographics(limit=limit), 
        "raw_demographics"
    )
    load_info.raise_on_failed_jobs()