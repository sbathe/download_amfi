import json


def load_config(conffile="config.json") -> dict:
    return json.load(open(conffile))
