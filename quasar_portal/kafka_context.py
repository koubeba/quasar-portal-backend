from pykafka.cluster import Cluster
from pykafka.handlers import ThreadingHandler
from pykafka.broker import Broker
from pykafka.topic import Topic
from pykafka.partition import Partition
from pykafka.balancedconsumer import BalancedConsumer
from typing import Dict, List, Optional
from pykafka.common import OffsetType
from itertools import islice

from .errors.topic_not_existing import TopicNotExisting


IN_PREFIX: str = 'in-'
OUT_PREFIX: str = 'out-'


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

    def get_connection_information(self) -> int:
        brokers: Dict[str, Broker] = self.__get_brokers()
        connected: int = 0
        for broker_key in brokers.keys():
            if brokers.__getitem__(broker_key).connected:
                connected += 1
        return connected

    def send_message(self, topic_name: str, message):
        with self.__get_topic(topic_name).get_sync_producer() as producer:
            producer.produce(message)

    def get_last_messages_offset(self, topic_name: str = 'test') -> int:
        consumer: BalancedConsumer = self.__get_topic(topic_name)\
            .get_balanced_consumer(consumer_group=b'portal',
                                   auto_offset_reset=OffsetType.LATEST,
                                   reset_offset_on_start=True)
        partition_names: List[str] = list(consumer.partitions.keys())
        if partition_names:
            partition: Partition = consumer.partitions.__getitem__(partition_names[0])
            return partition.latest_available_offset()
        return 0

    def get_last_messages(self, n: int, topic_name: str = 'test') -> str:
        consumer: BalancedConsumer = self.__get_topic(topic_name)\
            .get_balanced_consumer(consumer_group=b'portal',
                                   auto_offset_reset=OffsetType.LATEST,
                                   reset_offset_on_start=True)
        if consumer.held_offsets:
            n = min(max(consumer.held_offsets.values())+1, n)
            partitions: List[Partition] = consumer.partitions
            offsets = [(p, op - n)
                       for p, op in consumer.held_offsets.items()]
            # if we want to rewind before the beginning of the partition, limit to beginning
            offsets = [(partitions[p], (o if o > -1 else -2)) for p, o in offsets]
            # reset the consumer's offsets
            consumer.reset_offsets(offsets)
        result: Dict[int, bytes] = {}
        for message in islice(consumer, n):
            result[int(message.offset)] = message.value
        return str(result)

    def list_topics(self) -> List[str]:
        return [t.decode('utf-8') for t in list(self.client.topics.keys())]

    def list_in_topics(self, file_format: str) -> List[str]:
        return [topic for topic in self.list_topics() if (topic.startswith(IN_PREFIX) and topic.endswith(file_format.lower()))]

    def list_out_topics(self) -> List[str]:
        return [topic for topic in self.list_topics() if topic.startswith(OUT_PREFIX)]

    def __create_kafka_client(self) -> Cluster:
        print(f'Creating a Kafka Producer with bootstrap servers {self.__bootstrap_servers}')
        return Cluster(hosts=self.__bootstrap_servers,
                       handler=ThreadingHandler(),
                       zookeeper_hosts=self.__zookeper_servers)

    def __get_brokers(self) -> Dict[str, Broker]:
        return self.client.brokers

    def __get_topic(self, topic_name: str) -> Topic:
        topics = self.client.topics
        binary_topic_name = topic_name.encode('utf-8')
        if binary_topic_name not in topics:
            raise TopicNotExisting(topic_name)
        return topics[topic_name]
