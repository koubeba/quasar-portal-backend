from pykafka.cluster import Cluster
from pykafka.handlers import ThreadingHandler
from pykafka.broker import Broker
from pykafka.topic import Topic
from pykafka.partition import Partition
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
from typing import Any, Dict, List, Optional, Set
from pykafka.common import OffsetType
from itertools import islice
from datetime import datetime
from collections import defaultdict
import cachetools.func
from .errors.topic_not_existing import TopicNotExisting
from .errors.connection_error import BrokerConnectionError


from .file_format import FileFormat
from .topic_type import TopicType


class KafkaContext:
    def __init__(self, bootstrap_servers: str, zookeeper_servers: str):
        print(
            f'Creating Kafka Context with bootstrap servers {bootstrap_servers} and zookeeper servers {zookeeper_servers}...')
        self.__bootstrap_servers = bootstrap_servers
        self.__zookeper_servers = zookeeper_servers
        self.__client__: Optional[Cluster] = None

    @property
    @cachetools.func.ttl_cache(ttl=300)
    def cluster(self) -> Cluster:
        return self.__create_kafka_cluster()

    def get_connection_information(self) -> int:
        brokers: Dict[str, Broker] = self.__get_brokers()
        return sum([1 for broker_key in brokers.keys() if brokers.__getitem__(broker_key).connected])

    def send_message(self, topic_name: str, message):
        with self.__get_topic(topic_name).get_producer() as producer:
            try:
                producer.produce(message, timestamp=datetime.now())
            except (SocketDisconnectedError, LeaderNotAvailable):
                producer.stop()
                raise BrokerConnectionError()

    def get_last_messages_offset(self, topic_name: str = 'test') -> int:
        consumer: BalancedConsumer = self.__get_topic(topic_name) \
            .get_balanced_consumer(consumer_group=b'portal',
                                   auto_offset_reset=OffsetType.LATEST,
                                   reset_offset_on_start=True)
        partition_names: List[str] = list(consumer.partitions.keys())
        last_offsets: int = 0
        if partition_names:
            partition: Partition = consumer.partitions.__getitem__(partition_names[0])
            last_offsets = partition.latest_available_offset()
        consumer.stop()
        return last_offsets

    def get_in_topics_offsets(self) -> List[Dict[str, int]]:
        def format_topic_name(topic_name: str) -> str:
            return topic_name \
                .replace(FileFormat.CSV.to_suffix(), '') \
                .replace(FileFormat.JSON.to_suffix(), '') \
                .replace(TopicType.INCOMING.to_prefix(), '')

        in_topics: Set[str] = set(
            [format_topic_name(topic) for topic in self.list_topics() if (topic.startswith(TopicType.INCOMING.to_prefix()))])
        results: List[Dict[str, int]] = []

        for topic in in_topics:
            result: Dict[str, Any] = defaultdict(dict)
            result["topic_name"] = topic
            for format_name in [str(FileFormat.CSV), str(FileFormat.JSON)]:
                try:
                    result[format_name] = self.get_last_messages_offset(f'in-{topic}-{format_name}')
                except TopicNotExisting:
                    result[format_name] = 0
            results.append(result)

        return results

    def get_last_messages(self, n: int, topic_name: str = 'test') -> List[Dict[str, str]]:
        consumer: BalancedConsumer = self.__get_topic(topic_name) \
            .get_balanced_consumer(consumer_group=b'portal',
                                   auto_offset_reset=OffsetType.LATEST,
                                   reset_offset_on_start=True)
        if consumer.held_offsets:
            n = min(max(consumer.held_offsets.values()) + 1, n)
            partitions: List[Partition] = consumer.partitions
            offsets = [(p, op - n)
                       for p, op in consumer.held_offsets.items()]
            offsets = [(partitions[p], (o if o > -1 else -2)) for p, o in offsets]
            consumer.reset_offsets(offsets)
        result: List[Dict[str, str]] = []
        try:
            for message in islice(consumer, n):
                result.append({"value": message.value.decode('utf-8'),
                               "timestamp": message.timestamp})
        except SocketDisconnectedError:
            consumer.stop()
            raise BrokerConnectionError()
        finally:
            consumer.stop()
        return result

    def list_topics(self) -> List[str]:
        return [t.decode('utf-8') for t in list(self.cluster.topics.keys())]

    def list_in_topics(self, file_format: str = 'csv') -> List[str]:
        if not file_format or file_format == 'undefined':
            file_format: str = 'csv'
        return [topic for topic in self.list_topics() if
                (topic.startswith(TopicType.INCOMING.to_prefix()) and topic.endswith(file_format.lower()))]

    def list_out_topics(self) -> List[str]:
        return [topic for topic in self.list_topics() if topic.startswith(TopicType.OUTCOMING.to_prefix())]

    def __create_kafka_cluster(self) -> Cluster:
        print(f'Creating a Kafka Cluster with bootstrap servers {self.__bootstrap_servers}')
        return Cluster(hosts=self.__bootstrap_servers,
                       handler=ThreadingHandler(),
                       zookeeper_hosts=self.__zookeper_servers,
                       broker_version="1.0.0")

    def __get_brokers(self) -> Dict[str, Broker]:
        return self.cluster.brokers

    def __get_topic(self, topic_name: str) -> Topic:
        topics = self.cluster.topics
        binary_topic_name = topic_name.encode('utf-8')
        if binary_topic_name not in topics:
            raise TopicNotExisting(topic_name)
        return topics[topic_name]
