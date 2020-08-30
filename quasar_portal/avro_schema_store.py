import os
from typing import Dict, List
from .errors.schema_file import SchemaFileNotExisting, InvalidSchemaFile

import io
import avro.schema
import avro.io

SCHEMA_PATH: str = f'{os.path.dirname(os.path.realpath(__file__))}/schema'


def get_schema(schema_name: str, file_format: str) -> str:
    schema_filepath: str = f'{SCHEMA_PATH}/{schema_name}-{file_format.lower()}.json'
    if not os.path.exists(schema_filepath):
        raise SchemaFileNotExisting(schema_name)
    with open(schema_filepath, 'r') as schema_json_file:
        try:
            return schema_json_file.read()
        except ValueError:
            raise InvalidSchemaFile(schema_name)


class AvroEncoder:
    def __init__(self):
        self.__byte_writer: io.BytesIO = io.BytesIO()
        self.__encoder: avro.io.BinaryEncoder = avro.io.BinaryEncoder(self.__byte_writer)

    @property
    def encoder(self) -> avro.io.BinaryEncoder:
        return self.__encoder

    @property
    def byte_writer(self):
        return self.__byte_writer


AVRO_READER: AvroEncoder = AvroEncoder()


class AvroSchema:
    def __init__(self, schema_name: str, file_format: str):
        self.__encoder: AvroEncoder = AVRO_READER

        self.__schema_json: str = get_schema(schema_name, file_format)
        self.__schema = avro.schema.parse(self.__schema_json)
        self.__writer: avro.io.DatumWriter = avro.io.DatumWriter(self.__schema)
        self.__reader: avro.io.DatumReader = avro.io.DatumReader(self.__schema)

    @property
    def schema_json(self) -> Dict[str, str]:
        return self.__schema_json

    def encode(self, record: Dict[str, str]) -> bytes:
        self.__writer.write(record, self.__encoder.encoder)
        return self.__encoder.byte_writer.getvalue()

    def decode(self, record: bytes) -> Dict[str, str]:
        byte_reader: io.BytesIO = io.BytesIO(record)
        decoder: avro.io.BinaryDecoder = avro.io.BinaryDecoder(byte_reader)
        return self.__reader.read(decoder)


class AvroSchemaStore:
    def __init__(self, topics_list: List[str]):
        self.__topics = topics_list
        self.__topic_schemas: Dict[str, Dict[str, AvroSchema]] = {}
        for topic in self.__topics:
            self.__topic_schemas[topic] = {}
            self.__topic_schemas[topic]['CSV'] = AvroSchema(topic, 'CSV')
            self.__topic_schemas[topic]['JSON'] = AvroSchema(topic, 'JSON')

    def schema_for_topic(self, topic: str, file_format: str) -> Dict[str, str]:
        return self.__topic_schemas[topic][file_format].schema_json
