"""Parquet target class."""

from typing import Callable

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_parquet.sinks import ParquetSink
from target_parquet.writers import Writers


class TargetParquet(Target):
    """Sample target for Parquet."""

    name = "target-parquet"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "filepath", th.StringType, description="The path to the target output file"
        ),
        th.Property(
            "file_naming_scheme",
            th.StringType,
            description="The scheme with which output files will be named",
        ),
    ).to_dict()
    default_sink_class = ParquetSink

    def __del__(self):
        writers = Writers()
        writers.close_all()


if __name__ == "__main__":
    TargetParquet.cli()
