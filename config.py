import json

class Config:
    def load_config(self, conffile='config.json') -> dict:
        return json.load(open(conffile))
