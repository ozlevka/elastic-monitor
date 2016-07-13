

import sys
sys.path.append('../')

from agent import Configuration


def configuration_test():
    config = {
        'scheduler': {
            'interval': '10s'
        }
    }

    c = Configuration(config)

    print 'Get interval test ' +  str(c.get_interval() == '10s')





if __name__ == '__main__':
    configuration_test()