from flask import Flask
from typing import Optional
from .kafka_context import KafkaContext
import secrets


kafka_context: Optional[KafkaContext] = None


def create_app() -> Flask:
    app: Flask = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=secrets.token_urlsafe(32),
        KAFKA_BOOTSTRAP_SERVERS='35.241.252.88:9092',
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

    @app.route('/brokers')
    def brokers():
        return str(kafka_context.get_brokers())

    @app.route('/topics')
    def topics():
        return str(kafka_context.get_topics())

    # @app.route('/connected')
    # def connected():
    #     if kafka_context.producer_connected():
    #         return 'Connected'
    #     else:
    #         return 'Not Connected'

    @app.route('/send_message')
    def send_message():
        kafka_context.send_message(b'hello!!!')
        return 'sent!'

    return app
