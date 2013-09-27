'''
Created on Sep 26, 2013

@author: eric
'''

from . import ServiceInterface,GitServiceMarker #@UnresolvedImport

class GitHubService(ServiceInterface,GitServiceMarker):
    
    def __init__(self, user, password, org=None, **kwargs):
        self.org = org
        self.user = user
        self.password = password
    
        ur = 'https://api.github.com/'
        
        self.urls ={ 
                    'repos' : ur+'orgs/{}/repos'.format(self.org) if self.org else ur+'users/{}/repos'.format(self.user), 
                    'info' : ur+'repos/{}/{{name}}'.format(self.org)
                    }
        
        self.auth = (self.user, self.password)
 
    def has(self,name):
        import requests, json

        url = self.urls['info'].format(name=name)

        r = requests.get(url, auth=self.auth)
        
        if r.status_code != 200:
            return False
        else:
            return True

 
    def create(self, name):
        '''Create a new upstream repository'''
        import requests, json
        
        payload = json.dumps({'name':name})

        r = requests.post(self.urls['repos'], data=payload, auth=self.auth)
    
        if r.status_code != 200:
            raise Exception(r.json())
            
        else:
            return r.json()
    
    
    def ident(self):
        '''Return an identifier for this service'''
         
    def __str__(self):
        return "<GitHubService: user={} org={}>".format(self.user,self.org)
        