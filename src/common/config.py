import yaml

def load_config(config_path: str = "config/application.yaml") -> dict:
    with open(config_path, "r") as config_file:
        return yaml.safe_load(config_file)