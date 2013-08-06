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
        


def run(argv, bundle_class):

    bundle_class().run(argv)
                
if __name__ == '__main__':

    import sys, os, runpy

    args = list(sys.argv)

    bundle_file = sys.argv[1]
    
    rp = os.path.realpath(os.path.join(os.getcwd(), bundle_file))

    context = runpy.run_path(rp, run_name='__name__')
    
    #import pprint
    #pprint.pprint(context)
    
    dir_ = os.path.dirname(rp)
    
    context['Bundle'](dir_).run(args[2:])
    
   
    
    