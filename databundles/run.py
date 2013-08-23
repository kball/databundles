"""Runtime configuration logic for running a bundle build. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
from databundles.util import AttrDict
from databundles.util import lru_cache

@lru_cache()
def get_runconfig(path=None, is_server=False):
    return RunConfig(path, is_server)

class RunConfig(object):
    '''Runtime configuration object 
    
    The RunConfig object will search for a databundles.yaml file in multiple locations, 
    including::
    
      /etc/databundles.yaml
      ~user/.databundles.yaml
      ./databundles.yaml
      A named path ( --config option )
      
    It will start from the first directory, and for each one, try to load the file
    and copy the values into an accumulator, with later values overwritting
    earlier ones. 
    '''

    ROOT_CONFIG = '/etc/databundles/config.yaml'
    SERVER_CONFIG = '/etc/databundles/server.yaml'
    CLIENT_CONFIG = '/etc/databundles/client.yaml'
    USER_CONFIG = os.path.expanduser('~/.databundles.yaml')
    DIR_CONFIG = os.path.join(os.getcwd(),'databundles.yaml')

    def __init__(self, path=None, is_server = False):
        '''Create a new RunConfig object
        
        Arguments
        path -- If present, a yaml file to load last, overwriting earlier values.
          If it is an array, load only the files in the array. 

        '''
        
        self.config = AttrDict()
        self.config['loaded'] = []

    
        if isinstance(path, (list, tuple, set)):
            self.files = path
        else:
            self.files = [ 
                          RunConfig.SERVER_CONFIG if is_server else RunConfig.CLIENT_CONFIG , 
                          RunConfig.ROOT_CONFIG, 
                          RunConfig.USER_CONFIG, 
                          RunConfig.DIR_CONFIG, 
                          path]

        loaded = False

        for f in self.files:
            
            if f is not None and os.path.exists(f):
                try:
                    loaded = True
                    self.config.loaded.append(f)
                    self.config.update_yaml(f)
                except TypeError as e:
                    pass # Empty files will produce a type error

        if not loaded:
            raise Exception("Failed to load any config from: {}".format(self.files))

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        return self.config.get(group,{})

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self.config.get(name,{})

    def group_item(self, group, name):
        
        g = self.group(group)
        
        if not name in g:
            import pprint
            pprint.pprint(name)
            raise KeyError("Could not find name '{}' in group '{}'".format(name, group))
        
        return g[name]
        

    def _yield_string(self, e):
        '''Recursively descend a data structure to find string values.
         This will locate values that should be expanded by reference. '''
        from util import walk_dict

        for path, subdicts, values in walk_dict(e):
            for k,v in values:
                
                if v is None:
                    raise Exception('{} {} {} {} '.format(path, subdicts, k, v))
                
                path_parts = path.split('/')
                path_parts.pop()
                path_parts.pop(0)
                path_parts.append(k)
                def setter(nv):
                    sd = e
                    for pp in path_parts:
                        if not isinstance(sd[pp], dict ):
                            break
                        sd = sd[pp]
                        
                    # Save the oroginal value as a name
                        
                    sd[pp] = nv
                    
                    if isinstance(sd[pp], dict):
                        sd[pp]['_name'] = v
            
                yield k,v,setter
        
    
    def _sub_strings(self, e, subs):
        '''Substitute keys in the dict e with functions defined in subs'''

        iters = 0
        while (iters < 100):
            sub_count = 0
           
            for k,v,setter in self._yield_string(e):
                if k in subs:
                    setter(subs[k](k,v))
                    sub_count += 1

            if sub_count == 0:
                break

            iters += 1   
            
            
            
        return e         

    def dump(self, stream=None):
        
        to_string = False
        if stream is None:
            import StringIO
            stream = StringIO.StringIO()
            to_string = True
            
        self.config.dump(stream)
        
        if to_string:
            stream.seek(0)
            return stream.read()
        else:
            return stream
        

    def filesystem(self,name):
        e =  self.group_item('filesystem', name) 
        root_dir = e['root_dir'] if 'root_dir' in e  else  '/tmp/norootdir'
        from collections import  defaultdict

        d = defaultdict(str,{'root':root_dir} )

        return self._sub_strings(e, {
                                     'upstream': lambda k,v: self.filesystem(v),
                                     'account': lambda k,v: self.account(v),
                                     'dir' : lambda k,v: v.format(d)
                                     }  )
    
    def account(self,name):
        return  self.group_item('accounts', name) 


    def repository(self,name):
        e =  self.group_item('repository', name) 

        return self._sub_strings(e, {
                                     'filesystem': lambda k,v: self.filesystem(v)
                                     }  )
   
    def library(self,name):
        e =  self.group_item('library', name) 

        e =  self._sub_strings(e, {
                                     'filesystem': lambda k,v: self.filesystem(v),
                                     'remote': lambda k,v: self.filesystem(v),
                                     'database': lambda k,v: self.database(v) 
                                     }  )
     
        e['_name'] = name
     
        return e
     
    
    def warehouse(self,name):
        e =  self.group_item('warehouse', name) 

        return self._sub_strings(e, {
                                     'database': lambda k,v: self.database(v) 
                                     }  )
    def database(self,name):
        
        fs =  self.group_item('filesystem', name) 
        root_dir = fs['root_dir'] if 'root_dir' in fs  else  '/tmp/norootdir'
        
        e = self.group_item('database', name) 

        return self._sub_strings(e, {'dbname' : lambda k,v: v.format(root_dir=root_dir)}  )


def run(argv, bundle_class):

    raise Exception("Deprecated. Remove __main__ section from end of bundle.py")

def import_(filename):
    (path, name) = os.path.split(filename)
    (name, ext) = os.path.splitext(name)

    (file, filename, data) = imp.find_module(name, [path])
    return imp.load_module(name, file, filename, data)
                
if __name__ == '__main__':

    import sys, os, imp, pprint

    args = list(sys.argv)

    bundle_file = sys.argv[1]
    
    rp = os.path.realpath(os.path.join(os.getcwd(), bundle_file))
    dir_ = os.path.dirname(rp)

    mod = import_(rp)
 
    b = mod.Bundle(dir_)

    b.run(args[2:])
    
   
    
    