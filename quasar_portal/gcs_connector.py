from google.cloud import storage
from typing import Dict, List
import ndjson


def pretty_print_dirname(dirname: str) -> str:
    return dirname.replace('/', '').replace('_', ' ').title()


class GCSConnector:
    def __init__(self):
        self.__client = storage.Client()
        self.__schemas_bucket = self.__client.get_bucket("quasar_schemas")

    def list_models(self):
        model_types: List[str] = [pretty_print_dirname(blob.name) for blob in self.__client.list_blobs(
            "quasar_models", delimiter="/")]
        return model_types

    def fetch_schema(self, in_topic: str = "in", topic_name: str = 'test', file_format: str = 'CSV') -> str:
        file_name: str = f'{in_topic}/{file_format.lower()}/{topic_name}.json'
        blob = self.__schemas_bucket.get_blob(file_name)
        return (blob.download_as_string()).decode('utf-8')
