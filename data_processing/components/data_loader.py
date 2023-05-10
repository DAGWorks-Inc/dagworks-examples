import pandas as pd
from hamilton.function_modifiers import check_output, config, source
from hamilton.function_modifiers.adapters import load_from


@load_from.csv(path=source("order_details_path"), sep=",")
def order_details(df: pd.DataFrame) -> pd.DataFrame:
    return df


@load_from.csv(path=source("orders_path"), sep=",")
def orders(df: pd.DataFrame) -> pd.DataFrame:
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_list"] = df.apply(
        lambda x: [
            f"{x.product1}-{x.quantity1}",
            f"{x.product2}-{x.quantity2}",
            f"{x.product3}-{x.quantity3}",
        ],
        axis=1,
    )
    for x in ["product1", "product2", "product3", "quantity1", "quantity2", "quantity3"]:
        del df[x]
    df = df.explode("order_list")
    df["product_name"] = df["order_list"].apply(lambda x: x.split("-")[0])
    df["quantity"] = df["order_list"].apply(lambda x: int(x.split("-")[1]))
    del df["order_list"]
    return df
