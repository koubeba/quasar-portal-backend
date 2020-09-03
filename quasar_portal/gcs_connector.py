from google.cloud import storage
from typing import List, Optional


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

    def fetch_schema(self, topic_name: str, topic_prefix: str = "in", file_format: Optional[str] = None) -> str:
        if topic_prefix == "in" and file_format:
            file_name: str = f'in/{file_format.lower()}/{topic_name}.json'
        else:
            file_name: str = f'{topic_prefix}/{topic_name}.json'
        blob = self.__schemas_bucket.get_blob(file_name)
        return (blob.download_as_string()).decode('utf-8')
