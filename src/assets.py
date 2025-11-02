import polars as pl
from dagster import asset


@asset
def example_data():
    """A simple example asset that creates a sample dataset."""
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [100, 200, 300, 400, 500],
        }
    )
    return df


@asset(deps=[example_data])
def processed_data(example_data):
    """An asset that processes the example data."""
    # Double the values as an example transformation
    processed = example_data.with_columns(
        [(pl.col("value") * 2).alias("doubled_value")]
    )
    return processed
