"""
Module to transform iris data into features.
"""
import numpy as np
import pandas as pd
from hamilton.function_modifiers import parameterize_sources

RAW_FEATURES = ["sepal_length__cm_", "sepal_width__cm_", "petal_length__cm_", "petal_width__cm_"]

# Here is more terse code that does the same thing as the below *_log functions.
# Any `@parameterize*` decorator is just a less verbose way of defining functions that differ
# slightly. We don't see anything wrong with verbose code - so we recommend err'ing on the side of
# verbosity, but otherwise for this example show the terser code.
# @parameterize_sources(**{f"{col}_log": {"col": col} for col in RAW_FEATURES})
# def log_value(col: pd.Series) -> pd.Series:
#     """Log value of {col}."""
#     return np.log(col)


def sepal_length__cm__log(sepal_length__cm_: pd.Series) -> pd.Series:
    """Log value of sepal_length__cm_."""
    return np.log(sepal_length__cm_)


def sepal_width__cm__log(sepal_width__cm_: pd.Series) -> pd.Series:
    """Log value of sepal_width__cm_."""
    return np.log(sepal_width__cm_)


def petal_length__cm__log(petal_length__cm_: pd.Series) -> pd.Series:
    """Log value of petal_length__cm_."""
    return np.log(petal_length__cm_)


def petal_width__cm__log(petal_width__cm_: pd.Series) -> pd.Series:
    """Log value of petal_width__cm_."""
    return np.log(petal_width__cm_)


@parameterize_sources(**{f"{col}_mean": {"col": col} for col in RAW_FEATURES})
def mean_value(col: pd.Series) -> float:
    """Mean of {col}."""
    return col.mean()


@parameterize_sources(**{f"{col}_std": {"col": col} for col in RAW_FEATURES})
def std_value(col: pd.Series) -> float:
    """Standard deviation of {col}."""
    return col.std()


@parameterize_sources(
    **{
        f"{col}_normalized": {"col": col, "col_mean": f"{col}_mean", "col_std": f"{col}_std"}
        for col in RAW_FEATURES
    }
)
def normalized_value(col: pd.Series, col_mean: float, col_std: float) -> pd.Series:
    """Normalized column of {col}."""
    return (col - col_mean) / col_std


def data_set_v1(
    sepal_length__cm__normalized: pd.Series,
    sepal_width__cm__normalized: pd.Series,
    petal_length__cm__normalized: pd.Series,
    petal_width__cm__normalized: pd.Series,
    target_class: pd.Series,
) -> pd.DataFrame:
    """Explicitly define the feature set we want to use."""
    return pd.DataFrame(
        {
            "sepal_length__cm__normalized": sepal_length__cm__normalized,
            "sepal_width__cm__normalized": sepal_width__cm__normalized,
            "petal_length__cm__normalized": petal_length__cm__normalized,
            "petal_width__cm__normalized": petal_width__cm__normalized,
            "target_class": target_class,
        }
    )


def data_set_v2(
    sepal_length__cm__normalized: pd.Series,
    sepal_width__cm__normalized: pd.Series,
    petal_length__cm__normalized: pd.Series,
    petal_width__cm__normalized: pd.Series,
    sepal_length__cm__log: pd.Series,
    sepal_width__cm__log: pd.Series,
    petal_length__cm__log: pd.Series,
    petal_width__cm__log: pd.Series,
    target_class: pd.Series,
) -> pd.DataFrame:
    """Explicitly define the feature set we want to use. This one adds `log` features."""
    return pd.DataFrame(
        {
            "sepal_length__cm__normalized": sepal_length__cm__normalized,
            "sepal_width__cm__normalized": sepal_width__cm__normalized,
            "petal_length__cm__normalized": petal_length__cm__normalized,
            "petal_width__cm__normalized": petal_width__cm__normalized,
            "sepal_length__cm__log": sepal_length__cm__log,
            "sepal_width__cm__log": sepal_width__cm__log,
            "petal_length__cm__log": petal_length__cm__log,
            "petal_width__cm__log": petal_width__cm__log,
            "target_class": target_class,
        }
    )
