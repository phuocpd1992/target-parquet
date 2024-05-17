import datetime
import json
import os
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq


def get_date_string() -> str:
    return datetime.datetime.now().isoformat()[0:19].replace("-", "").replace(":", "")


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwds)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Writers(metaclass=SingletonMeta):
    _writers: dict = {}

    def start_writer(
            self,
            stream_name: str,
            schema: pa.Schema,
            filepath: str = None
    ):
        if self.exist_writer(stream_name):
            return

        filename = f"{stream_name}-{get_date_string()}.parquet"

        if filepath:
            filename = f"{filepath}/{filename}"

        self._writers[stream_name] = pq.ParquetWriter(
            filename, schema
        )

    def close_all(self):
        for value in self._writers.values():
            value.close()

        self._writers = {}

    def exist_writer(self, stream_name: str):
        return self._writers.get(stream_name) is not None

    def write(self, stream_name: str, row):
        writer = self._writers.get(stream_name)

        if not writer:
            return

        writer.write(row)

    def update_job_metrics(self, stream_name: str):
        job_metrics_path = "job_metrics.json"

        if not os.path.isfile(job_metrics_path):
            pathlib.Path(job_metrics_path).touch()

        with open(job_metrics_path, "r+") as f:
            content = dict()

            try:
                content = json.loads(f.read())
            except:
                pass

            if not content.get("recordCount"):
                content["recordCount"] = dict()

            content["recordCount"][stream_name] = (
                content["recordCount"].get(stream_name, 0) + 1
            )

            f.seek(0)
            f.write(json.dumps(content))
