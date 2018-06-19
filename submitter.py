#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import logging
import requests
from requests.exceptions import RequestException
import json


def _launch_module(docker_image, args, instances, ancoris_info):
    url = f"http://{ancoris_info['address']}:{ancoris_info['port']}{ancoris_info['base_url']}/tasks"
    data = {
        'image': docker_image,
        'args': args,
        'resources': {
            'cores': 1,
            'memory': 100,
            'swap': 0,
            'volumes': [],
            'ports': []
        },
        'opts': {
            'network_mode': 'host'
        },
        'events': {
            'on_exit': {
                'restart': True,
                'destroy': False
            }
        }
    }
    try:
        for i in range(1, instances + 1):
            response = requests.post(url, data=json.dumps(data))
            if response.status_code != 201:
                logging.error(f'Could not launch instance {i} for module {args[0]}. Response: {response.json()}')
                raise SystemExit
            logging.info(f'Launching instance {i} for module {args[0]}')
            logging.info(f'{docker_image}: {args}\n')
            logging.debug(response.json())
    except RequestException as e:
        logging.error(f'Error launching task: {args[0]}')


def deploy_topology(file_name):
    with open(file_name, 'r') as def_file:
        topology_conf = yaml.load(def_file)
    modules = topology_conf['modules']
    ext_conf = topology_conf['conf']

    conf_dict = {}
    for module, conf in modules.items():
        args = [module]
        args.extend(['-b', ext_conf['kafka']['address'] + ':' + str(ext_conf['kafka']['port'])])

        input_present = False
        if 'input' in conf:
            input_topics = ','.join(conf['input'])
            args.extend(['-i', input_topics])
            input_present = True

        output_present = False
        if 'output' in conf:
            output_topics = ','.join(conf['output'])
            args.extend(['-o', output_topics])
            output_present = True

        if not input_present and not output_present:
            logging.error('Input or output must be indicated.')
            raise SystemExit

        if 'aerospike' in conf:
            args.extend(['-a', ext_conf['aerospike']['address'] + ':' + str(ext_conf['aerospike']['port'])])
            if conf['aerospike']:
                args.extend(['-p', 'aerospike:' + conf['aerospike']['namespace'] + ':' + conf['aerospike']['set']])

        if 'instances' in conf:
            instances = conf['instances']

        _launch_module(conf['image'], args, instances, ext_conf['ancoris'])


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    deploy_topology('classifier.yaml')
    deploy_topology('tracker.yaml')
