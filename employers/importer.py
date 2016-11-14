import logging

import json
import re
import requests 

from xml_utils import fieldval, XmlReader
from utils import read_all_pages, get_auth_key
  
import_logger = logging.getLogger('import_log')
logger  = logging.getLogger('console')



class EmpClient(object):

    def __init__(self, url_base, key_secret):
        self.url_base = url_base
        self.key_secret = key_secret
        self.list_url = '{}/app/api/v1/employers/'.format(self.url_base)
        self.quick_list_url = '{}/app/api/v1/employers/quick-list'.format(self.url_base)

    def get_headers(self):
        return {
            'Authentication': 'CL {}'.format(self.key_secret),
            'Content-type': 'application/json'
        }

    def get_existing_ids(self):
        """ returns list of existing old ids in the CL databse,
            so we can skip them during the import 
        """        
        url = '{}?page_size={}'.format(self.quick_list_url, 250)
        headers = self.get_headers()
        ids = []
        for page in read_all_pages(url, headers):
            for item in page:
                old_id = item.get('old_id')
                if old_id:
                    ids.append(int(old_id))
        return ids 

    def save(self, data, id=None):
        headers = self.get_headers()

        if not id:
            url = self.list_url
            # logger.debug(url)
            res = requests.post(url, data=json.dumps(data), headers=headers)
        else:
            raise NotImplementedError("update is not implemented")

        if res.status_code not in [200, 201]: 
            logger.info('failed for record: %s, status_code=%s' % (data['name'], res.status_code))
            import_logger.error('failed to import data: %s', data)
            import_logger.error(res.content)            
            return False 

        #res.raise_for_status()
        #print 'saved: {}'.format(data['name'])
        return True





class Parser(object):
    def __init__(self, node, reader):
        self.node = node 
        company_id = self.id = int(fieldval(node, 'id'))
        self.users = reader.get_users_for_id(company_id)

    def record_identity(self):
        """ get identity for the failed record """
        for k in ['name', 'full_name', 'id']:
            val = fieldval(self.node, k)
            if val:
                return '{}={}'.format(k, val)

    def _fix_url(self, url):
        if url and not re.match(r'http(s)?:', url):
           url = 'http://{}'.format(url) 
        return url

    def get_data(self):
        n = self.node 
        company_name = fieldval(n, 'name')                

        if not company_name: 
            return

        users = [] 
        for full_name,email in self.users:            
            name_parts = full_name.split(' ')
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:])        
            u = {
                'first_name': first_name, 
                'last_name': last_name, 
                'email': email
            }
            users.append(u)

        assert users, "must have at least one user: %s" % self.id

        return {
            'name': company_name, 
            'old_id': self.id, 
            'url': self._fix_url(fieldval(n, 'url')), 
            'users': users
        }



from collections import defaultdict

class GroupUsersReader(object):
    """ quick modification on standard reader that reads though the data 
        and groups users 
        it introduces new method, that can be used for the data access 
    """
    def __init__(self, reader):
        self.reader = reader 
        users = self.users = defaultdict(list)

        nodes = self.nodes = [] 
        for node in reader.read():
            id = fieldval(node, 'id')      
            user = (fieldval(node, 'full_name'), fieldval(node, 'email'))

            if not id or not user[0] or not user[1]:
                continue
                        
            if not users.has_key(id):
                nodes.append(node)

            # print id, user 
            users[id].append(user)

    def get_users_for_id(self, id):
        #print 'get_users_for_id: %s, has_key: %s' %  (id, self.users.has_key(id))
        #print self.users.keys()
        return self.users[str(id)]

    def read(self):
        for node in self.nodes:
            yield node 




def cleanup_data(reader):
    # users are listed under the same name of the company/id
    # must be treated as users for the same company 
    return GroupUsersReader(reader)



def run(config, limit=None):
    # read all existing, don't send a request if data is already there 
    url = config.get('careerleaf', 'url')
    key_secret = key_secret =  get_auth_key(config) 
    file_name = config.get('employers', 'file')

    client = EmpClient(url, key_secret)
    
    reader = XmlReader(file_name)
    reader = cleanup_data(reader)

    
    total = 0
    success_count = 0
    skipped = 0
    existing = client.get_existing_ids()
    processed_ids = []
    for node in reader.read():
        parser = Parser(node, reader)
        id = parser.id 
        if id in existing:
            logger.debug('skipping : %s' % id)
            skipped +=1
            continue 

        assert not id in processed_ids 
        processed_ids.append(id) # ensuring that we do not process more than once       

        data = parser.get_data()

        if data:
            is_successful = client.save(data)
            if is_successful:
                success_count+=1
            logger.info('successfull for: {}'.format(data['name']))
        else:
            import_logger.error('failed, data problem for: {}'.format(parser.record_identity()))
        total +=1

        if limit and total > limit: 
            logger.info('reached the limit: {}, stopping'.format(limit) )
            break
        if total % 10 == 0:
            logger.info('processing record %s' % total)  

    logger.info('parsed {} records, {} are successfull, {} are failed, {} skipped'.format(total, success_count, (total - success_count), skipped))



# TODO: delete example    