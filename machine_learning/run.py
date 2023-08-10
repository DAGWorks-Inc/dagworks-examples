import datetime
import os
import click
import json

from dagworks import driver as dw_driver
from hamilton import base as h_base
from hamilton import driver as h_driver
from hamilton.function_modifiers import source
from hamilton.io.materialization import to

from components import iris_loader
from components import feature_transforms
from components import model_fitting
from components import models

import importlib
importlib.import_module("custom_materializers")


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
    dag_name = f"machine_learning_dag_{datetime.datetime.now().isoformat()}"
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
            username="elijah@dagworks.io",
            api_key=api_key,
            project_id=68,
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
    inputs = {
        "gamma": 0.001,
        "penalty": "l2",
        "solver": "lbfgs",
        "prefit_lr_clf_file": "prefit_lr_clf.pkl",
        "prefit_svm_clf_file": "prefit_svm_clf.pkl",
        "dataset_v2_save_file": "dataset_v2.csv",
    }
    materializers = [
        # to.pickle(
        #     dependencies=["best_model"],
        #     id="best_model_params_pkl",
        #     path=source("best_model_file")),
        # classificaiton report to .txt file
        # to.file(
        #     dependencies=["classification_report"],
        #     id="classification_report_to_txt",
        #     path=source("classification_report_file")),
        # # materialize the model to a pickle file
        to.pickle(
            dependencies=["lr_model.fit_clf"],
            id="fit_clf_lr_to_pickle",
            path=source("prefit_lr_clf_file"),
        ),
        to.pickle(
            dependencies=["svm_model.fit_clf"],
            id="fit_clf_svm_to_pickle",
            path=source("prefit_svm_clf_file"),
        ),
        # materialize the predictions we made to a csv file
        to.csv(
            dependencies=["data_set_v2"],
            id="dataset_v2_save",
            path=source("dataset_v2_save_file")
        ),
    ]
    # result = dr.execute(['best_model'], inputs=inputs)
    result = dr.materialize(
        *materializers,
        inputs=inputs
    )
    import pdb
    pdb.set_trace()
    
    print(result)


if __name__ == '__main__':
    run(auto_envvar_prefix='DAGWORKS')