import os
import click
import json

from dagworks import driver as dw_driver
from hamilton import base as h_base
from hamilton import driver as h_driver
from components import data_loader
from components import ftrs_autoregression
from components import ftrs_calendar
from components import ftrs_common_prep


from typing import Union

BASE_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "config")


def _load_config(config: Union[str, None]) -> dict:
    if config is None:
        return {}
    config_path = os.path.join(BASE_CONFIG_PATH, config + ".json")
    with open(config_path, "r") as f:
        return json.load(f)


@click.command()
@click.option("--dry-run", is_flag=True)
@click.option("--api-key", help="API key for the DAGWorks API. \
    This is taken from the env variable DAGWORKS_API_KEY if its not provided.", type=str)
def run(dry_run: bool, api_key: str, config: str=None):
    """
    Runs the time_series_feature_engineering hamilton DAG with the DAGWorks UI.

    :param dry_run: If true, the DAG will not be logged to DAGWorks
    """
    # Load the configuration file (optional). It is used to shape the DAG.
    config_loaded = _load_config(config)
    dag_name = f"time_series_feature_engineering_dag"
    # if config is not None:
    #    dag_name += f"_{config}"
    if api_key is None:
        api_key = os.environ.get("DAGWORKS_API_KEY", None)
    if not dry_run:
        dr = dw_driver.Driver(
            config_loaded,
            data_loader,
            ftrs_autoregression,
            ftrs_calendar,
            ftrs_common_prep,
            username="stefan@dagworks.io",
            api_key=api_key,
            project_id=31,
            dag_name=dag_name,
            tags={"template" : "time_series_feature_engineering", "TODO" : "add_more_tags_to_find_your_run_later"},

        )
    else:
        dr = h_driver.Driver(
             config_loaded,
             data_loader,
             ftrs_autoregression,
             ftrs_calendar,
             ftrs_common_prep,

        )
    inputs = {"data_path": "data/train_sample.csv"}
    all_possible_outputs = dr.list_available_variables()
    desired_features = [
        o.name for o in all_possible_outputs
        if o.tags.get("stage") == "production"]
    result = dr.execute(desired_features, inputs=inputs)
    
    print(result)


if __name__ == '__main__':
    run(auto_envvar_prefix='DAGWORKS')