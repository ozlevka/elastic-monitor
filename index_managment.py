

import sys
import yaml
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor



config = None
logger = None

class Manager:
    def __init__(self):
        pass


def run(cluster):
    logger.info(cluster['cluster']['host'])


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
    for cluster in config['elasticsearch']['slaves']:
        interval_unit = cluster['cluster']['scheduler']['units']
        interval_value = cluster['cluster']['scheduler']['interval']
        if interval_unit == 'seconds':
            scheduler.add_job(run, 'interval', seconds=interval_value, args=[cluster])
        elif interval_unit == 'minutes':
            scheduler.add_job(run, 'interval', minutes=interval_value, args=[cluster])
        elif interval_unit == 'hours':
            scheduler.add_job(run, 'interval', hours=interval_value, args=[cluster])


def main(argv):
    global config
    global logger
    f = open('./config/manager.yaml', mode='rb')
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