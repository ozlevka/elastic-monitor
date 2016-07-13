
import sys
import yaml
from elasticsearch import Elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler


class Elastic:
    def __init__(self, hosts):
        self.client = Elasticsearch(hosts)


class ElasticStatsReader:
    pass



class SystemStatsReader:
    pass



class ElasticWriter:
    pass



class Configuration:
    def __init__(self, config):
        self.config = config

    def get_interval(self):
        return self.config['scheduler']['interval']


def run():
    pass



def main(args):
    f = open('./config/config.yaml')
    conf = Configuration(yaml.load(f))
    f.close()





if __name__ == '__main__':
    main(sys.argv)