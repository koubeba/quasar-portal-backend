from flask import Flask, request
from flask_cors import CORS
from typing import Any, Optional
from .kafka_context import KafkaContext
import json
import secrets


kafka_context: Optional[KafkaContext] = None
ENCODING: str = 'utf-8'


def create_app() -> Flask:
    app: Flask = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=secrets.token_urlsafe(32),
        KAFKA_BOOTSTRAP_SERVERS='35.187.31.167:9092,34.78.25.50:9092,35.233.58.29:9092',
        ZOOKEEPER_SERVERS='35.187.31.167:2181'
    )
    CORS(app)

    from . import kafka_connector
    kafka_connector.init_app(app)

    @app.before_first_request
    def create_kafka_context():
        global kafka_context
        kafka_context = KafkaContext(bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
                                     zookeeper_servers=app.config['ZOOKEEPER_SERVERS'])

    @app.route('/')
    def index():
        return {'msg': 'Quasar Portal Backend'}

    @app.route('/connected', methods=['GET'])
    def connected():
        connected_brokers: int = kafka_context.get_connection_information()
        return {
            'data': {
                'connected_brokers': connected_brokers
            }
        }

    @app.route('/get_messages_offset', methods=['GET'])
    def get_messages_offset():
        return {
            'data': {
                'offset': kafka_context.get_last_messages_offset()
            }
        }

    @app.route('/get_messages', methods=['GET'])
    def get_messages():
        messages_count: int = request.args.get('count', default=10, type=int)
        return {
            'data': {
                'messages': kafka_context.get_last_messages(n=messages_count)
            }
        }

    @app.route('/get_sent_files_offset', methods=['GET'])
    def get_files_offset():
        return {
            'data': {
                'offset': kafka_context.get_last_sent_files_offset()
            }
        }

    @app.route('/get_sent_files', methods=['GET'])
    def get_sent_files():
        messages_count: int = request.args.get('count', default=10, type=int)
        return {
            'data': {
                'messages': kafka_context.get_last_sent_files(n=messages_count)
            }
        }

    @app.route('/send_message', methods=['POST'])
    def send_message():
        message: Optional[Any] = request.get_json()
        if message:
            kafka_context.send_message(json.dumps(message).encode(ENCODING))
            return 'Sent!'
        else:
            return 'No message was provided'

    @app.route('/send_file_data', methods=['POST'])
    def send_file_data():
        message: Optional[Any] = request.get_json()
        if message:
            kafka_context.send_file_data(json.dumps(message).encode(ENCODING))
            return 'Sent!'
        else:
            return 'No message was provided'

    return app
