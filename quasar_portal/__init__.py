from flask import Flask, request
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
        KAFKA_BOOTSTRAP_SERVERS='35.241.252.88:9092,34.78.25.50:9092,35.233.58.29:9092',
        ZOOKEEPER_SERVERS='35.241.252.88:2181'
    )

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

    @app.route('/get_messages', methods=['GET'])
    def get_messages():
        messages_count: int = request.args.get('count', default=10, type=int)
        return {
            'data': {
                'messages': kafka_context.get_last_messages(n=messages_count)
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

    return app
