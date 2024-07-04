import os
import click
import json

from dagworks import adapters
from hamilton import driver as h_driver
from hamilton import base
from components import data_loader
from components import common


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
@click.option("--config", required=False, default="config")
@click.option("--api-key", help="API key for the DAGWorks API. \
    This is taken from the env variable DAGWORKS_API_KEY if its not provided.", type=str)
def run(dry_run: bool, api_key: str, config: str=None):
    """
    Runs the data_processing hamilton DAG with the DAGWorks UI.

    :param dry_run: If true, the DAG will not be logged to DAGWorks
    """
    # Load the configuration file (optional). It is used to shape the DAG.
    config_loaded = _load_config(config)
    dag_name = f"data_processing_dag"
    if api_key is None:
        api_key = os.environ.get("DAGWORKS_API_KEY", None)
    adapter_list = [base.PandasDataFrameResult()]
    if not dry_run and api_key is not None:
        tracker = adapters.DAGWorksTracker(
            username="stefan@dagworks.io",
            api_key=api_key,
            project_id=32,
            dag_name=dag_name,
            tags={"template": "data_processing", "iteration": "1"},
        )
        adapter_list.append(tracker)
    dr = (
        h_driver.Builder()
        .with_config(config_loaded)
        .with_modules(
            data_loader,
            common,
        )
        .with_adapters(
            *adapter_list
        ).build()
    )
    inputs = {"order_details_path": "data/order_details.csv", "orders_path": "data/orders_new.csv"}
    result = dr.execute(['orders_by_order_aggregates'], inputs=inputs)
    
    print(result)


if __name__ == '__main__':
    run(auto_envvar_prefix='DAGWORKS')