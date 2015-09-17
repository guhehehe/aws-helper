#!/usr/bin/env python
# encoding: utf-8

import argparse
import logging
import sys
from   os.path import expanduser
import time
from   UserDict import IterableUserDict

import boto.ec2
from   boto.exception import EC2ResponseError
import yaml

logging.getLogger('boto').propagate = False

# AWS instance state code, from boto/ec2/instance.py
PENDING = 0
RUNNING = 16
SHUTTING_DOWN = 32
TERMINATED = 48
STOPPING = 64
STOPPED = 80

class InstancePool(object):
    def __init__(self, instance_ids, dry_run=False, blocking=True):
        """Initialization
        instance_ids: a list of instance ids
        blocking:     if true, the script will not exit until desired state is reached, for example, if you run start,
                      the script will not exit until all the instances is in `RUNNING` state
        """
        self._dry_run = dry_run
        self._blocking = blocking
        if not instance_ids:
            print 'No instance ids found.'
            sys.exit(1)
        try:
            self._conn = boto.ec2.connect_to_region("us-east-1")
        except NoAuthHandlerFound, e:
            print 'Authentication error, please make sure you have the correct boto config file.'
            sys.exit(1)

        try:
            self._instances = self._conn.get_only_instances(instance_ids=instance_ids)
        except EC2ResponseError, e:
            print 'Error connecting instances: {}'.format(e.message)
            sys.exit(1)

    def start(self):
        for instance in self._instances:
            instance_name = instance.tags['Name']

            if instance.state_code == RUNNING:
                print 'Instance {} is already running, skip it.'.format(instance_name)
                continue

            print 'Starting {}...'.format(instance_name)
            if not self._dry_run:
                instance.start()
                while self._blocking and self._get_status(instance.id) != RUNNING:
                    time.sleep(1)
                    continue

    def stop(self):
        for instance in self._instances:
            instance_name = instance.tags['Name']

            if instance.state_code == STOPPED:
                print 'Instance {} is already stopped, skip it.'.format(instance_name)
                continue

            print 'Stopping {}...'.format(instance_name)
            if not self._dry_run:
                instance.stop()
                while self._blocking and self._get_status(instance.id) != STOPPED:
                    time.sleep(1)
                    continue

    def reboot(self):
        for instance in self._instances:
            instance_name = instance.tags['Name']

            if instance.state_code != RUNNING:
                print 'Instance {} is not running, skip it.'.format(instance_name)
                continue

            print 'Rebooting {}...'.format(instance_name)
            if not self._dry_run:
                instance.reboot()
                while self._blocking and self._get_status(instance.id) != RUNNING:
                    time.sleep(1)
                    continue

    def state(self):
        for instance in self._instances:
            print 'Instance {} state: {}'.format(instance.tags['Name'], instance.state)

    def _get_status(self, instance_id):
        state = self._conn.get_all_instance_status(instance_ids=[instance_id], include_all_instances=True).pop()
        return state.state_code


class Config(IterableUserDict):
    """Load config file.

    Support two types of config files:
        user specific: ~/.imgr.yaml
        site wide: /etc/imgr.yaml

    If both file exist, the user specific file will overwirte the site wide one.

    The config file should be a yaml file in the following format:
        instance_group1: [instance_id_1, instance_id_2]
        instance_group2: [instance_id_3, instance_id_4]
    """
    config_file_path = dict(user='{}/.imgr.yaml'.format(expanduser('~')), site='/etc/imgr.yaml')

    def __init__(self):
        IterableUserDict.__init__(self)
        self.data = dict()
        self._load_site_config()
        self._load_user_config()

    def _load_user_config(self):
        self._load_config('user')

    def _load_site_config(self):
        self._load_config('site')

    def _load_config(self, type):
        try:
            with open(self.config_file_path[type], 'r') as f:
                self.data.update(yaml.safe_load(f))
        except Exception, e:
            logging.warn('Failed to load config file: {}'.format(e))


def parse_args():
    commands = ['start', 'stop', 'reboot', 'state']
    parser = argparse.ArgumentParser(description="AWS instances manager")
    parser.add_argument('-v', '--verbose', action='store_true', help='Print more information')
    parser.add_argument('-vv', '--nagging', action='store_true', help='Print even more information')
    parser.add_argument('-g', '--group', action='store_true', help='Use instance group from config file')
    parser.add_argument('-t', '--dry-run', action='store_true', help='Dry run')
    parser.add_argument('-n', '--non-blocking', action='store_true', help='Do not block')
    parser.add_argument('-d', '--delimiter', default=',', help='Delimiter to seperate instances')
    parser.add_argument('command', choices=commands, help='Command to run')
    parser.add_argument('instances', help='Instance ids seperated by --delimiter, if --group is enabled, this value \
            will be interpreted as instance group and the script will lookup the config file for the instance ids \
            under this group')

    return parser.parse_args()


def main():
    args = parse_args()
    level = logging.CRITICAL
    if args.verbose:
        level = logging.INFO
    if args.nagging:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    config = Config()
    instances = config.get(args.instances) if args.group else args.instances.split(args.delimiter)
    instance_pool = InstancePool(instances, dry_run=args.dry_run, blocking=False if args.non_blocking else True)
    func = getattr(instance_pool, args.command)
    try:
        func()
    except KeyboardInterrupt:
        print 'Keyboard interrupted'
        return 1


if __name__ == '__main__':
    sys.exit(main())
