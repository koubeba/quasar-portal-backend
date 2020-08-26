from flask import Flask, request
from typing import Optional
from .kafka_context import KafkaContext
import secrets


kafka_context: Optional[KafkaContext] = None


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

    @app.route('/connected')
    def connected():
        connected_brokers: int = kafka_context.get_connection_information()
        return {
            'data': {
                'connected_brokers': connected_brokers
            }
        }

    @app.route('/get_messages')
    def get_messages():
        return {
            'data': {
                'messages': kafka_context.get_messages()
            }
        }

    @app.route('/send_message')
    def send_message():
        kafka_context.send_message(b'hello!!!')
        return 'sent!'

    return app
