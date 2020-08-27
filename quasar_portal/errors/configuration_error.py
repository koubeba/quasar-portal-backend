class ConfigurationError(Exception):
    def __init__(self, keys, conf_file):
        self.message = f'No {keys.join(", ")} keys in the {conf_file} file!'
