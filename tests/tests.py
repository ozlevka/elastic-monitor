

import sys
sys.path.append('../')

import yaml
from agent import Configuration
from agent import ElasticStatsReader
from agent import SystemStatsReader
import socket


def configuration_test():
    config = {
        'scheduler': {
            'interval': {
                "units": 'seconds',
                'value': 10
            }
        }
    }

    c = Configuration(config)

    print 'Get interval test ' +  str(c.get_interval()['units'] == 'seconds' and c.get_interval()['value'] == 10)


def test_data_structures():
    f = open('../config/config.yaml')
    c = Configuration(yaml.load(f))
    f.close()
    sysreader = SystemStatsReader(c)
    estats = ElasticStatsReader(c)

    print sysreader.source.cluster.health()
    print estats.target.cluster.stats()
    print estats.target.indices.stats()


def test_system_data():
    f = open('../config/config.yaml')
    c = Configuration(yaml.load(f))
    f.close()
    sysreader = SystemStatsReader(c)
    print sysreader.read_data()


def test_index_system_data():
    f = open('../config/config.yaml')
    c = Configuration(yaml.load(f))
    f.close()
    sysreader = SystemStatsReader(c)
    print sysreader.index_data()

if __name__ == '__main__':
    #configuration_test()
    #test_data_structures()
    #test_system_data()
    test_index_system_data()