
import sys
import yaml
from elasticsearch import Elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler


class Elastic:
    def __init__(self, hosts):
        self.client = Elasticsearch(hosts)



class SimpleReader(object):
    '''
    Contains base types for Elasticsearch communictaion
    '''
    def __init__(self, config):
        self.target = Elasticsearch(config.get_elastic_target_host())
        self.source = Elasticsearch(config.get_elastic_source_host())


class ElasticStatsReader(SimpleReader):
    def __init__(self, config):
        super(ElasticStatsReader, self).__init__(config)



class SystemStatsReader(SimpleReader):
    def __init__(self, config):
        super(SystemStatsReader, self).__init__(config)




class ElasticWriter:
    def __init__(self):
        self.elastic = Elastic



class Configuration:
    def __init__(self, config):
        self.config = config

    def get_interval(self):
        return self.config['scheduler']['interval']

    def get_elastic_target_host(self):
        return self.config['elasticsearch']['target']['host'] + ':' + str(self.config['elasticsearch']['target']['port'])

    def get_elastic_source_host(self):
        return self.config['elasticsearch']['source']['host'] + ':' + str(self.config['elasticsearch']['source']['port'])


def run():
    pass



def main(args):
    f = open('./config/config.yaml')
    conf = Configuration(yaml.load(f))
    f.close()






if __name__ == '__main__':
    main(sys.argv)