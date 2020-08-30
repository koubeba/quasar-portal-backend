class SchemaFileNotExisting(Exception):
    def __init__(self, schema_name: str):
        self.message = f'{schema_name} not existing!'


class InvalidSchemaFile(Exception):
    def __init__(self, schema_name: str):
        self.message = f'{schema_name} is invalid!'
