import os
import click
import json

from dagworks import driver as dw_driver
from hamilton import base as h_base
from hamilton import driver as h_driver
from components import iris_loader
from components import feature_transforms
from components import model_fitting
from components import models


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
    Runs the machine_learning hamilton DAG with the DAGWorks UI.

    :param dry_run: If true, the DAG will not be logged to DAGWorks
    """
    # Load the configuration file (optional). It is used to shape the DAG.
    config_loaded = _load_config(config)
    dag_name = f"machine_learning_dag"
    # if config is not None:
    #    dag_name += f"_{config}"
    if api_key is None:
        api_key = os.environ.get("DAGWORKS_API_KEY", None)
    if not dry_run:
        dr = dw_driver.Driver(
            config_loaded,
            iris_loader,
            feature_transforms,
            model_fitting,
            models,
            username="stefan@dagworks.io",
            api_key=api_key,
            project_id=29,
            dag_name=dag_name,
            tags={"change_from_previous": "hyperparameter inputs"},
            adapter=h_base.SimplePythonGraphAdapter(h_base.DictResult()),
        )
    else:
        dr = h_driver.Driver(
             config_loaded,
             iris_loader,
             feature_transforms,
             model_fitting,
             models,
             adapter=h_base.SimplePythonGraphAdapter(h_base.DictResult()),
        )
    inputs = {"gamma": 0.001, "penalty": "l2", "solver": "lbfgs"}
    result = dr.execute(['best_model'], inputs=inputs)
    
    print(result)


if __name__ == '__main__':
    run(auto_envvar_prefix='DAGWORKS')