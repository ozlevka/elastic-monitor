
import sys
import yaml
from elasticsearch import Elasticsearch
import psutil
from datetime import datetime
import socket
import logging
from apscheduler.schedulers.blocking import BlockingScheduler


class SimpleReader(object):
    '''
    Contains base types for Elasticsearch comunictaion
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
        self.config = config

    def read_data(self):
        cpu = psutil.cpu_times()
        vm = psutil.virtual_memory()
        swm = psutil.swap_memory()
        dio = psutil.disk_io_counters()
        nio = psutil.net_io_counters(pernic=False)

        return {
            'host': socket.gethostname(),
            '@timestamp': datetime.now(),
            'cpu': {
                'user': cpu.user,
                'nice': cpu.nice,
                'system': cpu.system,
                'idle': cpu.idle,
                'iowait': cpu.iowait,
                'irq': cpu.irq,
                'softirq': cpu.softirq,
                'steal': cpu.steal,
                'guest': cpu.guest,
                'guest_nice': cpu.guest_nice
            },
            'mem': {
                'virtual': {
                    'total': vm.total,
                    'available': vm.available,
                    'percent': vm.percent,
                    'used': vm.used,
                    'free': vm.free,
                    'active': vm.active,
                    'inactive': vm.inactive,
                    'buffers': vm.buffers,
                    'cached': vm.cached,
                    'shared': vm.shared
                },
                'swap': {
                    'total': swm.total,
                    'used': swm.used,
                    'free': swm.free,
                    'percent': swm.percent,
                    'sin': swm.sin,
                    'sout': swm.sout
                }
            },
            'diskio': {
                'read_count': dio.read_count,
                'write_count': dio.write_count,
                'read_bytes': dio.read_bytes,
                'write_bytes': dio.write_bytes,
                'read_time': dio.read_time,
                'write_time': dio.write_time,
                'read_merged_count': dio.read_merged_count,
                'write_merged_count': dio.write_merged_count,
                'busy_time': dio.busy_time
            },
            "netio": {
                'bytes_sent': nio.bytes_sent,
                'bytes_recv': nio.bytes_recv,
                'packets_sent': nio.packets_sent,
                'packets_recv': nio.packets_recv,
                'errin': nio.errin,
                'errout': nio.errout,
                'dropin': nio.dropin,
                'dropout': nio.dropout
            }
        }

    def index_data(self):
        data = self.read_data()

        return self.target.index(self.config.get_target_index_name(), self.config.get_target_system_type(), data)


class Configuration:
    '''
    Contains information reader from config yaml
    '''
    def __init__(self, config):
        self.config = config

    def get_interval(self):
        return self.config['scheduler']['interval']

    def get_elastic_target_host(self):
        return self.config['elasticsearch']['target']['host'] + ':' + str(self.config['elasticsearch']['target']['port'])

    def get_elastic_source_host(self):
        return self.config['elasticsearch']['source']['host'] + ':' + str(self.config['elasticsearch']['source']['port'])

    def get_target_index_name(self):
        return self.config['elasticsearch']['target']['index']

    def get_target_system_type(self):
        return  self.config['elasticsearch']['target']['types']['system']



def run_system_only():
    global conf
    system = SystemStatsReader(config=conf)
    result = system.index_data()
    print


def main(args):
    run_flag = args[1]
    bs = BlockingScheduler()
    interval = conf.get_interval()
    bs.add_executor('processpool')
    if run_flag == 'sys':
        bs.add_job(run_system_only, 'interval', seconds=interval['value'])
    elif run_flag == 'elastic':
        pass
    else:
        pass
    bs.start()



if __name__ == '__main__':
    f = open('./config/config.yaml')
    conf = Configuration(yaml.load(f))
    f.close()
    logging.basicConfig()
    logging.getLogger('apscheduler.scheduler').setLevel(logging.DEBUG)
    main(sys.argv)