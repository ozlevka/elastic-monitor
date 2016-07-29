

import yaml
import os




class ConfigManager:
    def __init__(self):
        self.yaml = self.tryFromFile()

    def tryFromFile(self):
        try:
            f = open('./config/manager.yaml', mode='rb')
            y = yaml.load(f)
            f.close()
            return y
        except Exception, e:
            print e
            return None

    def get_config_general(self):
        pass
