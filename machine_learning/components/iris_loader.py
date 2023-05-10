"""
Module to load iris data.
"""
import pandas as pd
from hamilton.function_modifiers import extract_columns
from sklearn import datasets, utils

RAW_COLUMN_NAMES = [
    "sepal_length__cm_",
    "sepal_width__cm_",
    "petal_length__cm_",
    "petal_width__cm_",
]


def iris_data() -> utils.Bunch:
    return datasets.load_iris()


@extract_columns(*(RAW_COLUMN_NAMES + ["target_class"]))
def iris_df(iris_data: utils.Bunch) -> pd.DataFrame:
    _df = pd.DataFrame(iris_data.data, columns=RAW_COLUMN_NAMES)
    _df["target_class"] = [iris_data.target_names[t] for t in iris_data.target]
    return _df
