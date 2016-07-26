import sys
import os
import yaml
import logging
from fnmatch import fnmatch
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

config = None
logger = None

class Manager:
    def __init__(self, task):
        self.task = task
        self.client = self.createElasticClient(config['elasticsearch']['manager']['host'], config['elasticsearch']['manager']['port'])
        self.targetClient = None

    def createElasticClient(self, host, port):
        url =  host + ':' + str(port)
        return Elasticsearch(hosts=[url])

    def get_indices_data(self):
        res = self.client.search(index=config['elasticsearch']['manager']['index'])
        return [x['_source'] for x in res['hits']['hits']]

    def run_for_targets(self):
        indicies = self.get_indices_data()
        for index in indicies:
            self.run_for_index(index)

    def run_for_index(self, index):
        self.targetClient = self.createElasticClient(index['cluster']['host'], index['cluster']['port'])
        res = self.targetClient.indices.get_settings()
        for target in res:
            if fnmatch(target, index['template']):
                delete_date = self.calculate_delete_date(datetime.fromtimestamp(float(res[target]['settings']['index']['creation_date']) / 1000),
                                                         index['ttl'], index['units'])
                if datetime.now() > delete_date and not self.task['task']['debug']:
                    self.targetClient.indices.delete(target)
                else:
                    logger.debug(target + ' will be deleted in: ' + str(delete_date))

    def calculate_delete_date(self, create_date, ttl, units):
        if units == 'minutes':
            return create_date + timedelta(minutes=ttl)
        if units == 'hours':
            return  create_date + timedelta(hours=ttl)
        if units == 'months':
            return create_date + timedelta(days=(ttl * 30))
        if units == 'years':
            return create_date + timedelta(days=(ttl * 365))
        else:
            return create_date + timedelta(days=ttl)



def run(task):
    exec task['task']['code']





def get_logging_level():
    if config['logging']['level'] == 'DEBUG':
        return logging.DEBUG
    if config['logging']['level'] == 'INFO':
        return logging.INFO
    if config['logging']['level'] == 'WARN':
        return logging.WARN
    if config['logging']['level'] == 'ERROR':
        return logging.ERROR
    if config['logging']['level'] == 'FATAL':
        return logging.FATAL


def add_scheduler_tasks(scheduler):
    for task in config['manager']['tasks']:
        interval_unit = task['task']['interval']['units']
        interval_value = int(task['task']['interval']['value'])
        if interval_unit == 'seconds':
            scheduler.add_job(run, 'interval', seconds=interval_value, args=[task])
        elif interval_unit == 'minutes':
            scheduler.add_job(run, 'interval', minutes=interval_value, args=[task])
        elif interval_unit == 'hours':
            scheduler.add_job(run, 'interval', hours=interval_value, args=[task])


def main(argv):
    global config
    global logger
    f = open(os.environ['ES_MANAGER_CONFIG'], mode='rb')
    config = yaml.load(f)
    f.close()
    logging.basicConfig()
    logger = logging.getLogger(config['logging']['name'])
    logger.setLevel(get_logging_level())
    logging.getLogger('apscheduler.scheduler').setLevel(get_logging_level())

    scheduler = BlockingScheduler()
    scheduler.add_executor(ThreadPoolExecutor(5))
    add_scheduler_tasks(scheduler)
    scheduler.start()

if __name__ == '__main__':
    main(sys.argv)