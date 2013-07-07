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
        
def config_command(b, args):

    if args.subcommand == 'rewrite':
        b.log("Rewriting the config file")
        b.update_configuration()

def run(argv, bundle_class):

    b = bundle_class()
    args =  b.parse_args(argv)

    if args.command == 'config':
        config_command(b,args)
        return

    if hasattr(args,'clean') and args.clean:
        # If the clean arg is set, then we need to run  clean, and all of the
        # earlerier build phases. 
        ph = {
              'meta': ['clean'],
              'prepare': ['clean'],
              'build' : ['clean', 'prepare'],
              'install' : ['clean', 'prepare', 'build'],
              'submit' : ['clean', 'prepare', 'build'],
              'extract' : ['clean', 'prepare', 'build']
              }

        phases = ph.get(args.command,[]) + [args.command]
    else:
        phases = args.command

    if args.test:
        print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        print "!!!!!! In Test Mode !!!!!!!!!!"
        print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        import time
        time.sleep(1)

    if 'info' in phases:
        if args.schema:
            print b.schema.as_csv()
        else:
            b.log("----Info ---")
            b.log("Name : "+b.identity.name)
            b.log("VName: "+b.identity.vname)
            
            for partition in b.partitions:
                b.log("Partition: "+partition.name)
            
        return
    
    if 'run' in phases:
        #
        # Run a method on the bundle. Can be used for testing and development. 
        try:
            f = getattr(b,str(args.method))
        except AttributeError as e:
            b.error("Could not find method named '{}': {} ".format(args.method, e))
            b.error("Available methods : {} ".format(dir(b)))
      
            return
        
        if not callable(f):
            raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))
      
        return f()
       
    

    if 'clean' in phases:
        b.log("---- Cleaning ---")
        # Only clean the meta phases when it is explicityly specified. 
        b.clean(clean_meta=('meta' in phases))
        
    # The Meta phase prepares neta information, such as list of cites
    # that is doenloaded from a website, or a specificatoin for a schema. 
    # The meta phase does not require a database, and should write files
    # that only need to be done once. 
    if 'meta' in phases:
        if b.pre_meta():
            b.log("---- Meta ----")
            if b.meta():
                b.post_meta()
                b.log("---- Done Meta ----")
            else:
                b.log("---- Meta exited with failure ----")
                return False
        else:
            b.log("---- Skipping Meta ---- ")
    else:
        b.log("---- Skipping Meta ---- ") 
               
        
    if 'prepare' in phases:
        if b.pre_prepare():
            b.log("---- Preparing ----")
            if b.prepare():
                b.post_prepare()
                b.log("---- Done Preparing ----")
            else:
                b.log("---- Prepare exited with failure ----")
                return False
        else:
            b.log("---- Skipping prepare ---- ")
    else:
        b.log("---- Skipping prepare ---- ") 
        
    if 'build' in phases:
        
        if b.run_args.test:
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "!!!!!! In Test Mode !!!!!!!!!!"
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

            time.sleep(1)
            
        if b.pre_build():
            b.log("---- Build ---")
            if b.build():
                b.post_build()
                b.log("---- Done Building ---")
            else:
                b.log("---- Build exited with failure ---")
                return False
        else:
            b.log("---- Skipping Build ---- ")
    else:
        b.log("---- Skipping Build ---- ") 
    
    if 'install' in phases:
        if b.pre_install():
            b.log("---- Install ---")
            if b.install():
                b.post_install()
                b.log("---- Done Installing ---")
            else:
                b.log("---- Install exited with failure ---")
        else:
            b.log("---- Skipping Install ---- ")
    else:
        b.log("---- Skipping Install ---- ")      
     
    if 'extract' in phases:
        if b.pre_extract():
            b.log("---- Extract ---")
            if b.extract():
                b.post_extract()
                b.log("---- Done Extracting ---")
            else:
                b.log("---- Extract exited with failure ---")
        else:
            b.log("---- Skipping Extract ---- ")
    else:
        b.log("---- Skipping Extract ---- ")        
     
    # Submit puts information about the the bundles into a catalog
    # and may store extracts of the data in the catalog. 
    if 'submit' in phases:
        if b.pre_submit():
            b.log("---- Submit ---")
            if b.submit():
                b.post_submit()
                b.log("---- Done Submitting ---")
            else:
                b.log("---- Submit exited with failure ---")
        else:
            b.log("---- Skipping Submit ---- ")
    else:
        b.log("---- Skipping Submit ---- ")            
      

    if 'test' in phases:
        ''' Run the unit tests'''
        import nose, unittest, sys

        dir_ = b.filesystem.path('test') #@ReservedAssignment
                         
                   
        loader = nose.loader.TestLoader()
        tests =loader.loadTestsFromDir(dir_)
        
        result = unittest.TextTestResult(sys.stdout, True, 1) #@UnusedVariable
        
        print "Loading tests from ",dir_
        for test in tests:
            print "Running ", test
            test.context.bundle = b
            unittest.TextTestRunner().run(test)

                
    
    