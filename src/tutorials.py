"""
import prefect
from prefect import task, Flow
"""
import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db
from enum import Enum


### Prefect ###
"""
@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello World!")


with Flow("hello-world") as flow:
    hello_task()

flow.run()
"""

### Dask ###
index = pd.date_range("2021-09-01", periods=2400, freq="1H")
df = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)
ddf = dd.from_pandas(df, npartitions=10)


### Enum ###


class Danger(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
