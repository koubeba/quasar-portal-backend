class BrokerConnectionError(Exception):
    def __init__(self):
        self.message = f'Connection to broker lost!'
