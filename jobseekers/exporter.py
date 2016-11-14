import logging
import logging.config
import yaml

import json
import re
import os.path
import requests 

import ConfigParser
import codecs

from utils import get_config, read_all_pages, get_auth_key


import_logger = logging.getLogger('import_log')
logger  = logging.getLogger('console')


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
        self.save_dir = config.get('jobseekers', 'save_dir')

        self.list_url = '{}/app/api/v1/candidates'.format(self.url_base)

        self.save_profile_data = config_get_safe(config, 'jobseekers', 'save_profile_data',  True)
        self.import_limit = int(config_get_safe(config, 'jobseekers', 'import_limit') or 0)


    def get_headers(self):
        return {
            'Authentication': 'CL {}'.format(self.key_secret),
            'Content-type': 'application/json'
        }

    def download_file(self, url, name, prefix='', detect_extension=False):
        headers = self.get_headers()
        del headers['Content-type']
        url = '{}{}'.format(self.url_base, url)
        res = requests.get(url, headers=headers, stream=True)

        ctype = res.headers.get('content-type')
        ext = ''
        if detect_extension and ctype:
            ext = '.' + ctype.split('/')[1]

        file_name = u'{}{}{}'.format(prefix, name, ext)
        path = os.path.join(self.save_dir, file_name)

        if res.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in res:
                    f.write(chunk)
        else:
            logger.error('request failed {}: {}'.format(url, res.content))

        # import ipdb; ipdb.set_trace()


    def resume_download_url(self, prof_id, resume_id):
        return '/app/api/v1/candidates/{}/resumes/{}'.format(prof_id, resume_id)

    def already_imported(self, prefix):
        path = os.path.join(self.save_dir, prefix+"resume-auto.pdf")
        return os.path.exists(path)


    def save_record(self, cand):
        user = cand['user']
        prefix = u'{}_{}_{}_'.format(cand.get('id'), user['first_name'], user['last_name'])

        if self.already_imported(prefix):
            logger.debug(u'already saved {}, skipping'.format(prefix))
            return False
        # if os.exists()        
        logger.debug(u'processing: {}'.format(prefix))

        data_file = u'{}data.json'.format(prefix)

        with codecs.open(os.path.join(self.save_dir, data_file), 'w', encoding="utf-8") as out: 
            out.write(json.dumps(cand,  indent=4, ensure_ascii = False))

        # with open(os.path.join(self.save_dir, data_file), 'w') as out: 
        #     out.write(json.dumps(cand,  indent=4))

        prof = cand['profile']
        prof_id = cand['id']
        avatar_url = prof.get('photo_url')
        if avatar_url:
            self.download_file(avatar_url, "photo", prefix=prefix, detect_extension=True)

        auto_resume_url = self.resume_download_url(prof_id, 'auto')
        # print 'auto_resume_url=%s' % auto_resume_url
        self.download_file(auto_resume_url, "resume-auto.pdf", prefix=prefix)

        for res in cand.get('resumes'):
            resume_url = self.resume_download_url(prof_id, res['id'])
            self.download_file(resume_url, res['file_name'], prefix=prefix + "resume-")

        return True 

    def run(self):
        logger.debug('starting export, limit={}'.format(self.import_limit))
        url = '{}?page_size={}'.format(self.list_url, 1)
        headers = self.get_headers()
        ids = []
        counter = 0
        stop = False 
        for page in read_all_pages(url, headers):
            for cand in page:       
                if self.import_limit and counter > self.import_limit:
                    logger.debug('reached limit')
                    stop = True 
                    break 
                saved = self.save_record(cand)
                if saved: 
                    counter += 1 
            if stop:
                break

        logger.debug('completed, {} records processed'.format(counter))



    # def run(self):
    #     logger.debug('starting export, limit={}'.format(self.import_limit))
    #     url = '{}?page_size={}'.format(self.list_url, 1)
    #     headers = self.get_headers()
    #     ids = []
    #     counter = 1
    #     stop = False 
    #     for page in read_all_pages(url, headers, self.import_limit):
    #         for emp in page:
    #             if self.import_limit and counter > self.import_limit:
    #                 logger.debug('reached limit')
    #                 stop = True 
    #                 break 
    #             saved = self.save_record(emp)
    #             if saved:
    #                 counter += 1
    #         if stop:
    #             break

    #     logger.debug('completed, {} records saved'.format(counter-1))     


def run(config):

    # file_name = config.get('employers', 'file')

    client = ReaderClient(config)
    client.run()

