

import sys
sys.path.append('../')

import yaml
from agent import Configuration
from agent import ElasticStatsReader
from agent import SystemStatsReader


def configuration_test():
    config = {
        'scheduler': {
            'interval': '10s'
        }
    }

    c = Configuration(config)

    print 'Get interval test ' +  str(c.get_interval() == '10s')


def test_data_structures():
    f = open('../config/config.yaml')
    c = Configuration(yaml.load(f))
    f.close()
    sysreader = SystemStatsReader(c)
    estats = ElasticStatsReader(c)

    print sysreader.source.cluster.health()
    print estats.target.cluster.stats()


if __name__ == '__main__':
    #configuration_test()
    test_data_structures()