#!/usr/bin/env python
# encoding: utf-8


import argparse
import json
import sys
import time
import logging
import boto.ec2.elb as ELB
import boto.utils

from argparse import ArgumentDefaultsHelpFormatter as formatter
from boto.exception import NoAuthHandlerFound


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto').propagate = False


def parse_args():
    parser = argparse.ArgumentParser(description="Manage devices load balancers", formatter_class=formatter)
    parser.add_argument('-v', '--verbose', action='store_true', help='Print more information')
    parser.add_argument('--porcelain', action='store_true', help='Give the output in an easy-to-parse format for scripts.')
    subparser = parser.add_subparsers(title='Subcommands')

    subparser_ = subparser.add_parser('whoami', help='Describe current instance', formatter_class=formatter)
    subparser_.add_argument('-f', '--full', action='store_true', help='Print complete instance info')
    subparser_.set_defaults(func=whoami)

    subparser_ = subparser.add_parser('elb-all', help='List all elbs in a region, region defaults to us-east-1',
        formatter_class=formatter)
    subparser_.add_argument('-r', '--region', default='us-east-1', help='Region to list elbs for')
    subparser_.set_defaults(func=elb_all)

    subparser_ = subparser.add_parser('elb-joined', help='List all elbs the current instance is a member of',
        formatter_class=formatter)
    subparser_.set_defaults(func=elb_joined)

    subparser_ = subparser.add_parser('elb-members', help='List all instances currently added to the given elb',
        formatter_class=formatter)
    subparser_.add_argument('-r', '--region', default='us-east-1', help='Region this elb is in')
    subparser_.add_argument('name', help='Name of the elb')
    subparser_.set_defaults(func=elb_members)

    subparser_ = subparser.add_parser('elb-add', help=('Add the given instance, or current instance if not given, to'
        ' the given elb. This command blocks until the add has finished and the elb brings the instance online'),
        formatter_class=formatter)
    subparser_.add_argument('name', help='Name of the elb')
    subparser_.add_argument('-i', '--instance-id', dest='id', help='Instance id, or current instance\'s id if None')
    subparser_.add_argument('-r', '--region', default='us-east-1', help='Region this elb is in')
    subparser_.add_argument('-s', '--skip-prompt', dest='skip', action='store_true', help='Skip confirmation prompt')
    subparser_.set_defaults(func=elb_add)

    subparser_ = subparser.add_parser('elb-remove', help=('remove the given instance, or me if not given, from the'
        ' given elb. This command blocks until the remove has finished and the elb brings the instance online'),
        formatter_class=formatter)
    subparser_.add_argument('name', help='Name of the elb')
    subparser_.add_argument('-i', '--instance-id', dest='id', help='Instance id, or current instance\'s id if None')
    subparser_.add_argument('-r', '--region', default='us-east-1', help='Region this elb is in')
    subparser_.add_argument('-s', '--skip-prompt', dest='skip', action='store_true', help='Skip confirmation prompt')
    subparser_.set_defaults(func=elb_remove)

    subparser_ = subparser.add_parser('elb-health-rate', help=('Show the percentage of health instances in a given ELB'
        ' after removing the given instance'), formatter_class=formatter)
    subparser_.add_argument('name', help='Name of the elb')
    subparser_.add_argument('-r', '--region', default='us-east-1', help='Region this elb is in')
    subparser_.set_defaults(func=elb_health_rate)

    return parser.parse_args()


def whoami(args):
    if not args.porcelain and args.verbose:
        logging.debug('Collecting metadata for current instance...')
    metadata = boto.utils.get_instance_metadata(timeout=3, num_retries=3)
    if not metadata:
        raise Exception('I don\'t know who I am.')
    if not args.full:
        print metadata['instance-id']
        return
    # doing this due to some json format issue in some cases
    metadata = json.loads(json.dumps(metadata))
    print json.dumps(metadata, indent=2)


def elb_all(args):
    if not args.porcelain and args.verbose:
        logging.debug('Getting ELBs in region ({})'.format(args.region))
    elbs = ELBCollection(args.region).get_elbs()
    if not elbs:
        raise Exception('No ELBs found in region {}:'.format(args.region))
    if not args.porcelain:
        print 'ELBs found in region {}:'.format(args.region)
    for elb in elbs:
        print elb.name
    return


def elb_joined(args):
    if not args.porcelain and args.verbose:
        logging.debug('Collecting identity information for current instance...')
    identity = boto.utils.get_instance_identity(timeout=3, num_retries=3)
    if not identity:
        raise Exception('I don\'t know who I am.')

    region = identity['document']['region']
    instance_id = identity['document']['instanceId']
    if not args.porcelain and args.verbose:
        logging.debug('Current instance: ({}), Region: ({}).'.format(instance_id, region))

    elbs = ELBCollection(region)
    registered_elbs = elbs.get_elbs_registered_by(instance_id)
    if not registered_elbs:
        raise Exception('No ELBs are registered by me ({})'.format(instance_id))
    if not args.porcelain:
        print 'ELBs current instance ({}) was registered to:'.format(instance_id)
    if not args.porcelain and args.verbose:
        logging.debug('Getting ELBs current instance has registered to.')
    for elb in registered_elbs:
        print elb.name


def elb_members(args):
    elbs = ELBCollection(args.region)
    instances = elbs.get_instance_of(args.name)
    if not instances:
        raise Exception('No instances was found for ELB {} in region {}'.format(args.name, args.region))
    for instance in instances:
        print instance.id


def _confirmation(prompt):
    input = raw_input(prompt)
    return input.lower() in ['y', 'yes']


def _elb_has_instance(elb, instance_id):
    instance_ids = set(map(lambda x: x.id, elb.instances))
    return instance_id in instance_ids


def elb_add(args):
    if not args.id:
        print 'No instance id specified, using current instance id'
        identity = boto.utils.get_instance_identity(timeout=3, num_retries=3)
        if not identity:
            raise Exception('I don\'t know who I am.')

        region = identity['document']['region']
        instance_id = identity['document']['instanceId']
    else:
        region = args.region
        instance_id = args.id

    elbs = ELBCollection(region)
    elb = elbs.get_elb_by_name(args.name)

    if _elb_has_instance(elb, instance_id):
        raise Exception('{} was already in {}'.format(instance_id, args.name))

    if not args.skip:
        if not _confirmation('Do you want to add instance {} to ELB {}?\n'.format(instance_id, args.name)):
            print 'Operation aborted.'
            return

    print 'Adding instance {} from ELB {} in region {}'.format(instance_id, args.name, region)
    elb.register_instances([instance_id])

    while elbs.get_elb_by_name(args.name).get_instance_health([instance_id])[0].state == 'OutOfService':
        time.sleep(2)

    print '{} is added to {}'.format(instance_id, args.name)


def elb_remove(args):
    if not args.id:
        print 'No instance id specified, using current instance id'
        identity = boto.utils.get_instance_identity(timeout=3, num_retries=3)
        if not identity:
            raise Exception('I don\'t know who I am.')

        region = identity['document']['region']
        instance_id = identity['document']['instanceId']
    else:
        region = args.region
        instance_id = args.id

    elbs = ELBCollection(region)
    elb = elbs.get_elb_by_name(args.name)

    if not _elb_has_instance(elb, instance_id):
        raise Exception('{} was not registered to {}'.format(instance_id, args.name))

    if not args.skip:
        if not _confirmation('Do you want to remove instance {} from ELB {}?\n'.format(instance_id, args.name)):
            print 'Operation aborted.'
            return

    print 'Removing instance {} from ELB {} in region {}'.format(instance_id, args.name, region)
    elb.deregister_instances([instance_id])

    while elbs.get_elb_by_name(args.name).get_instance_health([instance_id])[0].state != 'OutOfService':
        time.sleep(2)

    print '{} is removed from {}'.format(instance_id, args.name)


def elb_health_rate(args):
    elbs = ELBCollection(args.region)
    healths = elbs.get_elb_by_name(args.name).get_instance_health()
    all_instances = len(healths)
    if not all_instances:
        raise Exception('No instances are attached to this ELB: {}'.format(args.name))

    health_num = 0
    for health in healths:
        if health.state == 'InService':
            health_num += 1
    print 100 * (health_num - 1) / all_instances


class ELBCollection(object):
    """A collection of ELBs in the given region"""

    def __init__(self, region):
        self.region = region
        self.connection = None
        self.elbs = None

    def _get_connection(self):
        if not self.connection:
            connection = ELB.connect_to_region(self.region)
            if not connection:
                raise Exception('Couldn\'t connection to region {}'.format(self.region))
            self.connection = connection
        return self.connection

    def get_elb_by_name(self, elb_name):
        for elb in self.get_elbs():
            if elb.name == elb_name:
                return elb
        raise Exception('ELB {} was not found in region {}'.format(elb_name, self.region))

    def get_elbs(self):
        connection = self._get_connection()
        if not self.elbs:
            self.elbs = self.connection.get_all_load_balancers()
        return self.elbs

    def get_elbs_registered_by(self, instance_id):
        results = []
        for elb in self.get_elbs():
            for instance in elb.instances:
                if instance.id == instance_id:
                    results.append(elb)
                    continue
        return results

    def get_instance_of(self, elb_name):
        return self.get_elb_by_name(elb_name).instances


def main():
    args = None
    try:
        args = parse_args()
        args.func(args)
        return
    except NoAuthHandlerFound, e:
        logging.error('Authentication error, please make sure you have the correct boto config file.')
    except KeyboardInterrupt, e:
        print 'Aborted.'
    except Exception, e:
        if not args:
            raise e
        if args.verbose:
            logging.exception(e)
        else:
            logging.error(e)
    return 1


if __name__ == '__main__':
    sys.exit(main())
