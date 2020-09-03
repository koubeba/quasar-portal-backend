from enum import Enum, auto


class FileFormat(Enum):
    CSV = auto()
    JSON = auto()

    def __str__(self):
        return self.name.lower()

    def to_suffix(self):
        return f'-{self.name.lower()}'
