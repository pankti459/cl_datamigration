import logging
import logging.config
import yaml

import json
import re
import os.path
import requests 

import ConfigParser

from utils import get_config, read_all_pages, get_auth_key

logger  = logging.getLogger('import_log')

def config_get_safe(config, section, key, default=None):
    try:
        return config.get(section, key)
    except ConfigParser.NoOptionError:
        return default


def disable_ssl_warning():
    import requests.packages.urllib3
    requests.packages.urllib3.disable_warnings()    


class ReaderClient(object):
    def __init__(self, config):
        self.url_base = config.get('careerleaf', 'url')
        if self.url_base.startswith('https'):
            disable_ssl_warning()

        self.key_secret = get_auth_key(config) 
        self.save_dir = config.get('employers', 'save_dir')

        self.list_url = '{}/app/api/v1/employers/'.format(self.url_base)
        self.quick_list_url = '{}/app/api/v1/employers/quick-list'.format(self.url_base)

        self.save_profile_data = config_get_safe(config, 'employers', 'save_profile_data',  True)
        self.import_limit = int(config_get_safe(config, 'employers', 'import_limit') or 0)


    def get_headers(self):
        return {
            'Authentication': 'CL {}'.format(self.key_secret),
            'Content-type': 'application/json'
        }


    def save_record(self, emp):
        name = emp.get('name').replace('/', '_').replace('\0','_')
        empid = emp.get('id').replace('/','_').replace('\0','_')
        prefix = u'{}_{}'.format(name, empid)

        logger.debug(u'processing: {}'.format(prefix))

        data_file = u'{}data.json'.format(prefix)
        with open(os.path.join(self.save_dir, data_file), 'w') as out: 
            out.write(json.dumps(emp,  indent=4))
        return True 

    def run(self):
        logger.debug('starting export, limit={}'.format(self.import_limit))
        url = '{}?page_size={}'.format(self.list_url, 1)
        headers = self.get_headers()
        ids = []
        counter = 1
        stop = False 
        for page in read_all_pages(url, headers, self.import_limit):
            for cand in page:
                if self.import_limit and counter > self.import_limit:
                    print "counter inside--",counter
                    logger.debug('reached limit')
                    stop = True 
                    break 
                saved = self.save_record(cand)
                if saved:
                    counter += 1
            if stop:
                break

        logger.debug('completed, {} records saved'.format(counter-1))        


def run(config):
    client = ReaderClient(config)
    client.run()

