
import sys
import yaml
from elasticsearch import Elasticsearch
import psutil
import copy
from datetime import datetime
import socket
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor


class SimpleReader(object):
    '''
    Contains base types for Elasticsearch comunictaion
    '''
    def __init__(self, config):
        self.target = Elasticsearch(config.get_elastic_target_host())
        self.source = Elasticsearch(config.get_elastic_source_host())

    def calculate_difference(self, data, prev, parent_key):
        for k in data[parent_key]:
            data[parent_key][k] = data[parent_key][k] - prev[parent_key][k]


class ElasticStatsReader(SimpleReader):
    def __init__(self, config):
        super(ElasticStatsReader, self).__init__(config)



class SystemStatsReader(SimpleReader):
    def __init__(self, config):
        logging.getLogger('apscheduler.scheduler').info('execute reader constructor')
        super(SystemStatsReader, self).__init__(config)
        self.config = config
        self.logger = logging.getLogger('system.reader')
        self.logger.setLevel(config.get_logging_level())
        self.previouse = None

    def read_data(self):
        cpu = psutil.cpu_times()
        vm = psutil.virtual_memory()
        swm = psutil.swap_memory()
        dio = psutil.disk_io_counters()
        nio = psutil.net_io_counters(pernic=False)

        return {
            'host': socket.gethostname(),
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
        if self.previouse == None:
            self.previouse = copy.deepcopy(data)
        else:
            index_data = self.merge(data)
            index_data['@timestamp'] = datetime.utcnow()
            self.logger.info(str(self.target.index(self.config.get_target_index_name(), self.config.get_target_system_type(), index_data)))

    def merge(self, data):
        prev = copy.deepcopy(self.previouse)
        self.previouse = copy.deepcopy(data)
        self.calculate_difference(data, prev, 'cpu')
        self.calculate_difference(data, prev, 'diskio')
        self.calculate_difference(data, prev, 'netio')
        return data




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
        return self.config['elasticsearch']['target']['index']['name'] + "_" + datetime.utcnow().strftime(self.config['elasticsearch']['target']['index']['dateformat'])

    def get_target_system_type(self):
        return  self.config['elasticsearch']['target']['types']['system']

    def get_logging_level(self):
        if self.config['logging']['level'] == 'DEBUG':
            return logging.DEBUG
        if self.config['logging']['level'] == 'INFO':
            return logging.INFO
        if self.config['logging']['level'] == 'WARN':
            return logging.WARN
        if self.config['logging']['level'] == 'ERROR':
            return logging.ERROR
        if self.config['logging']['level'] == 'FATAL':
            return logging.FATAL



def run_system_only(sys_reader):
    sys_reader.index_data()


def add_job_to_scheduler(scheduler, job_func, args):
    global conf
    interval_unit = conf.get_interval()['units']
    interval_value = conf.get_interval()['value']
    if interval_unit == 'second':
        scheduler.add_job(job_func, 'interval', seconds=interval_value, args=args)
    elif interval_unit == 'minute':
        scheduler.add_job(job_func, 'interval', minutes=interval_value, args=args)
    elif interval_unit == 'hour':
        scheduler.add_job(job_func, 'interval', hours=interval_value, args=args)


def main(args):
    run_flag = args[1]
    bs = BlockingScheduler()
    bs.add_executor(ThreadPoolExecutor(5))
    system = SystemStatsReader(conf)
    if run_flag == 'sys':
        add_job_to_scheduler(bs,run_system_only, [system])
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
    logging.getLogger('apscheduler.scheduler').setLevel(conf.get_logging_level())
    main(sys.argv)