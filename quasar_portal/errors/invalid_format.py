class InvalidFormat(Exception):
    def __init__(self, format_name: str):
        self.message = f'Cannot resolve format {format_name}!'
