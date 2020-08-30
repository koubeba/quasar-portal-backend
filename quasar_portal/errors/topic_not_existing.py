class TopicNotExisting(Exception):
    def __init__(self, topic_name: str):
        self.__topic = topic_name
        self.message = f'Topic {topic_name} not existing!'

    @property
    def topic(self) -> str:
        return self.topic
