from flask import Flask, request
from flask_caching import Cache
from flask_cors import CORS
from typing import Any, Dict, List, Optional, Tuple
from .kafka_context import KafkaContext
from .gcs_connector import GCSConnector
from .errors.configuration_error import ConfigurationError
from .errors.topic_not_existing import TopicNotExisting
import json
import secrets
import os


kafka_context: Optional[KafkaContext] = None
gcs_connector: Optional[GCSConnector] = None
ENCODING: str = 'utf-8'
KAFKA_BROKERS_KEY: str = "kafka_brokers"
ZOOKEEPER_SERVER_KEY: str = "zookeeper_server"
CONF_KEYS: List[str] = [KAFKA_BROKERS_KEY, ZOOKEEPER_SERVER_KEY]


BAD_REQUEST: int = 400


def read_json_configuration() -> Dict[str, str]:
    with open(f'{os.path.dirname(os.path.realpath(__file__))}/kafka_configuration.json') as conf:
        data = json.load(conf)

    if not all(k in data for k in CONF_KEYS):
        raise ConfigurationError([KAFKA_BROKERS_KEY])
    return data


def handle_topic_not_existing(topic_name: str) -> Tuple[Dict[str, str], int]:
    return ({
        'error': f'Topic {topic_name} not existing'
    }, BAD_REQUEST)


def create_app() -> Flask:
    app: Flask = Flask(__name__, instance_relative_config=True)
    configuration: Dict[str, str] = read_json_configuration()
    app.config.from_mapping(
        SECRET_KEY=secrets.token_urlsafe(32),
        KAFKA_BOOTSTRAP_SERVERS= configuration[KAFKA_BROKERS_KEY],
        ZOOKEEPER_SERVERS= configuration[ZOOKEEPER_SERVER_KEY]
    )
    cache = Cache(config={'CACHE_TYPE': 'simple'})
    CORS(app)
    cache.init_app(app)

    @app.before_first_request
    def create_context():
        global kafka_context, gcs_connector
        kafka_context = KafkaContext(bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
                                     zookeeper_servers=app.config['ZOOKEEPER_SERVERS'])
        gcs_connector = GCSConnector()

    @app.route('/')
    def index():
        return {'msg': 'Quasar Portal Backend'}

    @app.route('/connected', methods=['GET'])
    @cache.cached(timeout=60)
    def connected():
        connected_brokers: int = kafka_context.get_connection_information()
        return {
            'data': {
                'connected_brokers': connected_brokers
            }
        }

    @app.route('/list_all_topics', methods=['GET'])
    @cache.cached(timeout=60)
    def list_all_topics():
        return {
            'data': {
                'topics': kafka_context.list_topics()
            }
        }

    @app.route('/list_in_topics', methods=['GET'])
    @cache.cached(timeout=60)
    def list_in_topics():
        file_format: str = request.args.get('format', type=str)
        return {
            'data': {
                'topics': kafka_context.list_in_topics(file_format)
            }
        }

    @app.route('/list_out_topics', methods=['GET'])
    @cache.cached(timeout=60)
    def list_out_topics():
        return {
            'data': {
                'topics': kafka_context.list_out_topics()
            }
        }

    @app.route('/list_models', methods=['GET'])
    @cache.cached(timeout=300)
    def list_models():
        return {
            'data': {
                'models': gcs_connector.list_models()
            }
        }

    @app.route('/get_schema', methods=['GET'])
    @cache.cached(timeout=300)
    def get_schema():
        topic_name: str = request.args.get('topic', type=str)
        in_out, topic_name = topic_name.split('-', 1)
        try:
            if in_out.lower() == 'in':
                topic_name, file_format = topic_name.rsplit('-', 1)
            else:
                file_format = None
            return {
                'data': {
                    'schema': gcs_connector.fetch_schema(in_topic=in_out,
                                                             topic_name=topic_name,
                                                             file_format=None)
                }
            }
        except TopicNotExisting:
            error_json, error_code = handle_topic_not_existing(topic_name)
            return error_json, error_code
        # TODO: add format error if anything else than CSV or JSON

    @app.route('/get_in_topics_offsets', methods=['GET'])
    def get_in_topics_offsets():
        return {
            'data': {
                'topic_offsets': kafka_context.get_in_topics_offsets()
            }
        }

    @app.route('/get_messages_offset', methods=['GET'])
    def get_messages_offset():
        topic_name: str = request.args.get('topic', type=str)
        try:
            return {
                'data': {
                    'offset': kafka_context.get_last_messages_offset(topic_name)
                }
            }
        except TopicNotExisting:
            error_json, error_code = handle_topic_not_existing(topic_name)
            return error_json, error_code

    @app.route('/get_messages', methods=['GET'])
    def get_messages():
        topic_name: str = request.args.get('topic', type=str)
        messages_count: int = request.args.get('count', default=10, type=int)
        try:
            return {
                "data": {
                    "messages": kafka_context.get_last_messages(topic_name=topic_name, n=messages_count)
                }
            }
        except TopicNotExisting:
            error_json, error_code = handle_topic_not_existing(topic_name)
            return error_json, error_code

    @app.route('/send_message', methods=['POST'])
    def send_message():
        topic_name: str = request.args.get('topic', type=str)
        message: Optional[Any] = request.get_data()
        if message:
            try:
                kafka_context.send_message(topic_name, message)
            except TopicNotExisting:
                error_json, error_code = handle_topic_not_existing(topic_name)
                return error_json, error_code
            return {
                'data': {
                    'topic': topic_name
                }
            }
        else:
            return 'No message was provided', BAD_REQUEST

    return app
