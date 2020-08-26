from pykafka.cluster import Cluster
from pykafka.handlers import ThreadingHandler
from typing import List, Optional

import json


class KafkaContext:
    def __init__(self, bootstrap_servers: str, zookeeper_servers: str):
        print(f'Creating Kafka Context with bootstrap servers {bootstrap_servers} and zookeeper servers {zookeeper_servers}...')
        self.__bootstrap_servers = bootstrap_servers
        self.__zookeper_servers = zookeeper_servers
        self.__client__: Optional[Cluster] = None

    @property
    def client(self) -> Cluster:
        if not self.__client__:
            self.__client__ = self.__create_kafka_client()
        return self.__client__

    def client_connected(self) -> bool:
        pass

    def get_brokers(self) -> List[str]:
        return self.client.brokers

    def get_topics(self) -> List[str]:
        return self.client.topics

    def send_message(self, message):
        topics = self.client.topics
        topic = topics.__getitem__(b'test')
        print(topic.name)
        print(topic.latest_available_offsets())
        with topic.get_sync_producer() as producer:
            producer.produce(message)

    def __create_kafka_client(self) -> Cluster:
        print(f'Creating a Kafka Producer with bootstrap servers {self.__bootstrap_servers}')
        return Cluster(hosts=self.__bootstrap_servers,
                       handler=ThreadingHandler(),
                       zookeeper_hosts=self.__zookeper_servers)
