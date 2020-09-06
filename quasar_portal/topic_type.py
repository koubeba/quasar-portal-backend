from enum import Enum, auto


class TopicType(Enum):
    INCOMING = auto()
    OUTCOMING = auto()

    def __str__(self):
        return self.name.lower().replace("coming", "")

    def to_prefix(self) -> str:
        return f'{self.name.lower().replace("coming", "")}-'
