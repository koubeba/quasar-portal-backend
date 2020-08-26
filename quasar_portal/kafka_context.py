from kafka import KafkaProducer
from typing import Optional

import json


class KafkaContext:
    def __init__(self, bootstrap_servers: str):
        print(f'Creating Kafka Context with bootstrap servers {bootstrap_servers}...')
        self.__bootstrap_servers = bootstrap_servers
        self.__producer__: Optional[KafkaProducer] = None

    @property
    def producer(self) -> KafkaProducer:
        if not self.__producer__:
            self.__producer__ = self.__create_kafka_producer()
        return self.__producer__

    def producer_connected(self) -> bool:
        return self.producer.bootstrap_connected()

    def __create_kafka_producer(self) -> KafkaProducer:
        print(f'Creating a Kafka Producer with bootstrap servers {self.__bootstrap_servers}')
        return KafkaProducer(bootstrap_servers=self.__bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
