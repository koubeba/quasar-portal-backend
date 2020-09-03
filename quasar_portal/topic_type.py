from enum import Enum, auto


class TopicType(Enum):
    INCOMING = auto()
    OUTCOMING = auto()

    def __str__(self):
        return self.name.lower()

    def to_prefix(self):
        return f'{self.name.lower()}-'
