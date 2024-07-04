import os
import click
import json

from dagworks import adapters
from hamilton import base as h_base
from hamilton import driver as h_driver
from components import transforms


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
    Runs the hello_world hamilton DAG with the DAGWorks UI.

    :param dry_run: If true, the DAG will not be logged to DAGWorks
    """
    # Load the configuration file (optional). It is used to shape the DAG.
    config_loaded = _load_config(config)
    dag_name = f"hello_world_dag"
    # if config is not None:
    #    dag_name += f"_{config}"
    if api_key is None:
        api_key = os.environ.get("DAGWORKS_API_KEY", None)

    adapter_list = [h_base.PandasDataFrameResult()]
    if not dry_run and api_key is not None:
        tracker = adapters.DAGWorksTracker(
            username="stefan@dagworks.io",
            api_key=api_key,
            project_id=2,
            dag_name=dag_name,
            #  - change_from_previous: one or more of -> code,config,data,None.
            #  - code_change: function_name(s)
            #  - config_change: config_name(s)
            #  - data_change: data_name(s)a
            tags={
                "change_from_previous": "code, output",
                "code_change": "added_function",
                "config_change": "None",
                "data_change": "None",
            },
        )
        adapter_list.append(tracker)

    dr = (
        h_driver.Builder()
        .with_config(config_loaded)
        .with_modules(
            transforms,
        )
        .with_adapters(
            *adapter_list
        ).build()
    )
    inputs = {"signups_path": "data/signups.csv", "spend_path": "data/spend.csv"}
    result = dr.execute(['spend', 'signups',
                         'avg_3wk_spend',
                         'spend_per_signup', 'spend_mean', 'spend_zero_mean', 'spend_std_dev', 'spend_zero_mean_unit_variance'], inputs=inputs)
    
    print(result)


if __name__ == '__main__':
    run(auto_envvar_prefix='DAGWORKS')