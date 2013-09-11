"""The Bundle object is the root object for a bundle, which includes acessors 
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from .identity import Identity 
from .filesystem import  BundleFilesystem
from .schema import Schema
from .partitions import Partitions

import os.path
from .dbexceptions import  ConfigurationError, ProcessError
from .run import get_runconfig

def get_identity(path):
    '''Get an identity from a database, either a bundle or partition'''
    from .database.sqlite import SqliteBundleDatabase #@UnresolvedImport
    
    db = SqliteBundleDatabase(path)
    

    bdc = BundleDbConfig(db)
    
    type_ = bdc.get_value('info','type')
    
    if type_ == 'bundle':
        return  bdc.dataset.identity 
    elif type_ == 'partition':
        return  bdc.partition.identity 
    else:
        raise Exception("Invalid type: {}", type)
    

    
class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
 
    logger = None
 
    def __init__(self, logger=None):
        '''
        '''

        self._schema = None
        self._partitions = None
        self._library = None
        self._identity = None
        self._repository = None

        if not logger:
            from .util import get_logger
            self.logger = get_logger(__name__)
        else:
            self.logger = logger 
            
        import logging
        self.logger.setLevel(logging.INFO) 
        
        
    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema(self)
            
        return self._schema
    
    @property
    def partitions(self):     
        if self._partitions is None:
            self._partitions = Partitions(self)  
            
        return self._partitions

    @property
    def repository(self):
        '''Return a repository object '''
        from databundles.repository import Repository
        from databundles.dbexceptions import ConfigurationError       
        
        if not self._repository:
            repo_name = 'default'
            self._repository =  Repository(self, repo_name)
            
        return self._repository
    
    @property
    def identity(self):
        '''Return an identity object. '''

        if not self._identity:
            
            self._identity =  Identity(**self.config.identity)
            
        return self._identity            
        
    @property
    def dataset(self):
        '''Return the dataset'''
        
        from databundles.orm import Dataset
 
        s = self.database.session

        return  (s.query(Dataset).one())
        
    @property
    def library(self):
        '''Return the library set for the bundle, or 
        local library from get_library() if one was not set. '''
          
        import library
        
        if self._library:
            l = self._library
        else:
            l =  library.new_library(self.config.config.library('default'))
            
        l.logger = self.logger
        l.database.logger = self.logger
        l.bundle = self
        return l

    @library.setter
    def library(self, value):
        self._library = value

    @property
    def path(self):
        """Return the base path for the bundle, usually the path to the
        bundle database, but withouth the database extension."""
        raise NotImplementedError("Abstract")

    def sub_dir(self, *args):
        """Return a subdirectory relative to the bundle's database root path
        which based on the path of the database. For paths relative to the
        directory of a BuildBundle, use the Filesystem object. """
        return  os.path.join(self.path,*args)
    
    def query(self,*args, **kwargs):
        """Convience function for self.database.connection.execute()"""
        return self.database.query(*args, **kwargs)
    
    def log(self, message, **kwargs):
        '''Log the messsage'''
        self.logger.info(message)


    def error(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.error(message)

    
class DbBundle(Bundle):

    def __init__(self, database_file, logger=None):
        '''Initialize a bundle and all of its sub-components. 
        
        If it does not exist, creates the bundle database and initializes the
        Dataset record and Config records from the bundle.yaml file. Through the
        config object, will trigger a re-load of the bundle.yaml file if it
        has changed. 
        
        Order of operations is:
            Create bundle.db if it does not exist
        '''
        from .database.sqlite import SqliteBundleDatabase #@UnresolvedImport
        import os
        
        super(DbBundle, self).__init__(logger=logger)
       
        self.database_file = database_file


        self.database = SqliteBundleDatabase(self, database_file)
        self.db_config = self.config = BundleDbConfig(self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. 
        
        self.run_args = None
        
    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        import os
        return os.path.join(self.path, *args) 
        
    def table_data(self, query):
        '''Return a petl container for a data table'''
        import petl 
        query = query.strip().lower()
        
        if 'select' not in query:
            query = "select * from {} ".format(query)
 
        
        return petl.fromsqlite3(self.database.path, query) #@UndefinedVariable
      

class BuildBundle(Bundle):
    '''A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration '''

    META_COMPLETE_MARKER = '.meta_complete'

    def __init__(self, bundle_dir=None):
        '''
        '''
        
        super(BuildBundle, self).__init__()
        

        if bundle_dir is None:
            import inspect
            bundle_dir = os.path.abspath(os.path.dirname(inspect.getfile(self.__class__)))
          
        if bundle_dir is None or not os.path.isdir(bundle_dir):
            from databundles.dbexceptions import BundleError
            raise BundleError("BuildBundle must be constructed on a cache. "+
                              str(bundle_dir) + " is not a directory")
  
        self.bundle_dir = bundle_dir
    
        self._database  = None
   
        # For build bundles, always use the FileConfig though self.config
        # to get configuration. 
        self.config = BundleFileConfig(self.bundle_dir)

     
        self.filesystem = BundleFilesystem(self, self.bundle_dir)
        

        import base64
        self.logid = base64.urlsafe_b64encode(os.urandom(6)) 
        self.ptick_count = 0;

        lib_dir = self.filesystem.path('lib')
        if os.path.exists(lib_dir):
            import sys
            sys.path.append(lib_dir)

        self._build_time = None
        self._update_time = None

    @property
    def path(self):
        return self.filesystem.path(
                    self.filesystem.BUILD_DIR,
                    self.identity.path) 

    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        return self.filesystem.path(self.filesystem.BUILD_DIR, self.identity.path,  *args) 

    @property
    def database(self):
        from .database.sqlite import BuildBundleDb #@UnresolvedImport
        if self._database is None:
            self._database  = BuildBundleDb(self, self.path)

        return self._database

    @property
    def db_config(self):
        return BundleDbConfig(self.database)

    def update_configuration(self):

        # Re-writes the undle.yaml file, with updates to the identity and partitions
        # sections. 
        self.config.rewrite(
                         identity=self.identity.to_dict(),
                         partitions=[p.identity.name for p in self.partitions]
                         )
        
        # Reload some of the values from bundle.yaml into the database configuration

        if self.config.build.get('dependencies'):
            dbc = self.db_config
            for k,v in self.config.build.get('dependencies').items():
                dbc.set_value('dependencies', k, v)
        
    @classmethod
    def rm_rf(cls, d):
        
        if not os.path.exists(d):
            return

        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)
       
    @property 
    def sources(self):
        """Return a dictionary of sources from the build.sources configuration key"""
        from databundles.dbexceptions import ConfigurationError
        
        if not self.config.group('build'): 
            raise ConfigurationError("Configuration does not have 'build' group")
        if not self.config.group('build').get('sources',None): 
            raise ConfigurationError("Configuration does not have 'build.sources' group")
            
        return self.config.build.sources
        
    def source(self,name):
        """Return a source URL with the given name, from the build.sources configuration
        value"""
        
        s = self.config.build.sources
        return s.get(name, None)
        
    
        

    def clean(self, clean_meta=False):
        '''Remove all files generated by the build process'''
        import os
        self.rm_rf(self.filesystem.build_path())
        
        if clean_meta:
            mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
            if os.path.exists(mf):
                os.remove(mf)
        
        ed = self.filesystem.path('extracts')
        if os.path.exists(ed):
            self.rm_rf(ed)
        
        # Should check for a shared download file -- specified
        # as part of the library; Don't delete that. 
        #if not self.cache_downloads :
        #    self.rm_rf(self.filesystem.downloads_path())

    

    def progress(self,message):
        '''print message to terminal, in place'''
        print 'PRG: ',message

    def ptick(self,message):
        '''Writes a tick to the stdout, without a space or newline'''
        import sys
        sys.stdout.write(message)
        sys.stdout.flush()
        
        self.ptick_count += len(message)
       
        if self.ptick_count > 72:
            sys.stdout.write("\n")
            self.ptick_count = 0

    def init_log_rate(self, N=5000, message=''):
        """Initialze the log_rate function. Returnas a partial function to call for
        each event"""
      
        import functools 
        d =  [0,  # number of items processed
                None, # start time
                N,  #frequency to log a message
                message]

        return functools.partial(self._log_rate, d)

    
    def _log_rate(self,d, message=None):
        """Log a message for the Nth time the method is called.
        
        d is the object returned from init_log_rate
        """
        
        import time 
    
        if not d[1]:
            d[1] = time.time()
    
        if not message:
            message = d[3]
    
        d[0] += 1
        if d[0] % d[2] == 0:
            # Prints the processing rate in 1,000 records per sec.
            self.log(message+': '+str(int( d[0]/(time.time()-d[1])))+'/s '+str(d[0]/1000)+"K ") 
        

    ### Prepare is run before building, part of the devel process.  

    def pre_meta(self):
        '''Skips the meta stage if the :class:.`META_COMPLETE_MARKER` file already exists'''
        import os.path
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
      
        if os.path.exists(mf):
            self.log("Meta information already generated")
            #raise ProcessError("Bundle has already been prepared")
            return False
        return True

    def meta(self):
        return True
    
    def post_meta(self):
        '''Create the :class:.`META_COMPLETE_MARKER` meta marker so we don't run the meta process again'''
        import datetime
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        with open(mf,'w+') as f:
            f.write(str(datetime.datetime.now()))
    
        return True

    ### Prepare is run before building, part of the devel process.  

    def pre_prepare(self):

        if not vars(self.run_args).get('rebuild',False) and  self.database.exists() and self.db_config.get_value('process','prepared'):
            self.log("Bundle has already been prepared")
            #raise ProcessError("Bundle has already been prepared")
     
            return False
        return True

    def prepare(self):

        if not self.database.exists():
            self.database.create()

        if vars(self.run_args).get('rebuild',False):
            self.rebuild_schema()
        else:
            sf  = self.filesystem.path(self.config.build.get('schema_file', 'meta/schema.csv'))
            if os.path.exists(sf):
                with open(sf, 'rbU') as f:
                    self.schema.clean()
                    self.schema.schema_from_file(f)      

        return True
    
    def rebuild_schema(self):
        sf  = self.filesystem.path(self.config.build.get('schema_file', 'meta/schema.csv'))
        with open(sf, 'rbU') as f:
            from databundles.orm import Partition
            partitions = [p.identity for p in self.partitions.all]
            self.schema.clean()
            self.schema.schema_from_file(f)  
            
            s = self.database.session
            for p in partitions:
                self.partitions.new_db_partition(p)
            s.commit()
    
    def post_prepare(self):
        '''Set a marker in the database that it is already prepared. '''
        from datetime import datetime
        self.db_config.set_value('process','prepared',datetime.now().isoformat())
        self.update_configuration()

        sf  = self.filesystem.path('meta','schema-revised.csv')

        with open(sf, 'w') as f:
            self.schema.as_csv(f)
                        
        return True
   
    ### Build the final package

    def pre_build(self):
        from time import time
        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")
        
        if not self.db_config.get_value('process','prepared'):
            raise ProcessError("Build called before prepare completed")
        
        self._build_time = time()
        
        return True
        
    def build(self):
        return False
    
    def post_build(self):
        from datetime import datetime
        from time import time
        self.db_config.set_value('process', 'built', datetime.now().isoformat())
        self.db_config.set_value('process', 'buildtime',time()-self._build_time)
        self.update_configuration()
        return True
    
        
    ### Update is like build, but calls into an earlier version of the package. 

    def pre_update(self):
        from time import time
        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")
        
        if not self.db_config.get_value('process','prepared'):
            raise ProcessError("Update called before prepare completed")
        
        self._update_time = time()
        
        return True
        
    def update(self):
        return False
    
    def post_update(self):
        from datetime import datetime
        from time import time
        self.db_config.set_value('process', 'updated', datetime.now().isoformat())
        self.db_config.set_value('process', 'updatetime',time()-self._update_time)
        self.update_configuration()
        return True
        
    ### Submit the package to the library
 
    def pre_install(self):
        
        self.update_configuration()
        
        return True
    
    def install(self, library_name=None):  
        '''Install the bundle and all partitions in the default library'''
     
        import databundles.library

        library_name = vars(self.run_args).get('library', 'default') if library_name is None else 'default'
        library_name = library_name if library_name else 'default'

        library = databundles.library.new_library(self.config.config.library(library_name))
     
        self.log("Install bundle {} to  library {}".format(self.identity.name, library_name))  
        dest = library.put(self)
        self.log("Installed to {} ".format(dest[1]))
        
        skips = self.config.group('build').get('skipinstall',[])
        
        for partition in self.partitions:
            
            if partition.name in skips:
                self.log('Skipping: {}'.format(partition.name))
            else:
                self.log("Install partition {}".format(partition.name))  
                dest = library.put(partition)
                self.log("Installed to {} ".format(dest[1]))

        return True
        
    def post_install(self):
        from datetime import datetime
        self.db_config.set_value('process', 'installed', datetime.now().isoformat())
        return True
    
    ### Submit the package to the repository
 
    def pre_submit(self):
        self.update_configuration()
        return True
    
    ### Submit the package to the repository
    def submit(self):
    
        self.repository.submit(root=self.run_args.name, force=self.run_args.force, 
                               repo=self.run_args.repo)
        return True
    
    def post_submit(self):
        from datetime import datetime
        self.db_config.set_value('process', 'submitted', datetime.now().isoformat())
        return True

    ### Submit the package to the repository
 
    def pre_extract(self):
        return True
    
    ### Submit the package to the repository
    def extract(self):
        self.repository.extract(root=self.run_args.name, force=self.run_args.force)
        return True
    
    def post_extract(self):
        from datetime import datetime
        self.db_config.set_value('process', 'extracted', datetime.now().isoformat())
        return True
    
    
    def repopulate(self):
        '''Pull bundle files from the library back into the working directory'''
        import shutil
        
        args = self.run_args
        
        self.log('---- Repopulate ----')
        
        b = self.library.get(self.identity.name)
        
        self.log('Copy bundle')
        shutil.copy(b.database.path, self.database.path)
         
        # Restart with the new bundle database.
        newb = BuildBundle(self.bundle_dir)
        
        for newp in newb.partitions:
            self.log('Copy partition: {}'.format(newp.identity.name))
            oldp = b.partitions.find(newp.identity.name)
            
            dir_ = os.path.dirname(newp.database.path);
            
            if not os.path.isdir(dir_):
                os.makedirs(dir_)
            
            shutil.copy(oldp.database.path, newp.database.path)

    def parse_args(self,argv):

        self.run_args = self.args_parser.parse_args(argv)
            
        return self.run_args
    
    @property
    def args_parser(self):
    
        import argparse
        import multiprocessing
        
        parser = argparse.ArgumentParser(prog='dbundle',
                                         description='Manage a DataBundle')
        
        # Commands: meta, prepare, build, install, extract, submit, 
        
        #parser.add_argument('command', nargs=1, help='Create a new bundle') 
     
        parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
        parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
        parser.add_argument('-r','--reset',  default=False, action="store_true",  help='')
        parser.add_argument('-t','--test',  default=False, action="store_true", help='Enable bundle-specific test behaviour')
        parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")
    
        parser.add_argument('-m','--multi',  type = int,  nargs = '?',
                            default = None,
                            const = multiprocessing.cpu_count(),
                            help='Run the build process on multiple processors, if the  method supports it')
    
        # These are args that Aptana / PyDev adds to runs. 
        parser.add_argument('--port', default=None, help="PyDev Debugger arg") 
        parser.add_argument('--verbosity', default=None, help="PyDev Debugger arg") 
    
        cmd = parser.add_subparsers(title='commands', help='command help')
        
       
 
        command_p = cmd.add_parser('config', help='Operations on the bundle configuration file')
        command_p.set_defaults(command='config')
           
        asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundl file')
    
        sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
        sp.set_defaults(subcommand='rewrite')
       
        #
        # Clean Command
        #
        command_p = cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
        command_p.set_defaults(command='clean')   
       
        #
        # Meta Command
        #
        command_p = cmd.add_parser('meta', help='Build or install metadata')
        command_p.set_defaults(command='meta')   
        
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')     
        
        #action='append_const', const='meta', dest='command', 
                                   
        
        #
        # Prepare Command
        #
        command_p = cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
        command_p.set_defaults(command='prepare')   
        
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        command_p.add_argument('-r','--rebuild', default=False,action="store_true", help='Rebuild the schema, but dont delete built files')
        
        #
        # Build Command
        #
        command_p = cmd.add_parser('build', help='Build the data bundle and partitions')
        command_p.set_defaults(command='build')   
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        
        command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')

    

        #
        # Update Command
        #
        command_p = cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
        command_p.set_defaults(command='update')   
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')


        #
        # Extract Command
        #
        command_p = cmd.add_parser('extract', help='Extract data into CSV and TIFF files. ')
        command_p.set_defaults(command='extract')   
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
        command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')


        #
        # Submit Command
        #
        command_p = cmd.add_parser('submit', help='Submit extracts to the repository ')
        command_p.set_defaults(command='submit')    
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')   
        command_p.add_argument('-r','--repo',  default=None, help='Name of the repository, defined in the config file')
        command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
        command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
        #
        # Install Command
        #
        command_p = cmd.add_parser('install', help='Install bundles and partitions to the library')
        command_p.set_defaults(command='install')  
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        command_p.add_argument('-l','--library',  help='Name of the library, defined in the config file')
             
        
        #
        # run Command
        #
        command_p = cmd.add_parser('run', help='Run a method on the bundle')
        command_p.set_defaults(command='run')               
        command_p.add_argument('method', metavar='Method', type=str, 
                       help='Name of the method to run')    
        command_p.add_argument('args',  nargs='*', type=str,help='additional arguments')
    
        #
        # info command
        #
        command_p = cmd.add_parser('info', help='Print information about the bundle')
        command_p.set_defaults(command='info')               
        command_p.add_argument('-s','--schema',  default=False,action="store_true",
                               help='Dump the schema as a CSV. The bundle must have been prepared')
         
        #
        # repopulate
        #
        command_p = cmd.add_parser('repopulate', help='Load data previously submitted to the library back into the build dir')
        command_p.set_defaults(command='repopulate')               

        return parser


    def run(self, argv):

        b = self
        args =  b.parse_args(argv)
    
        if args.command == 'config':
            if args.subcommand == 'rewrite':
                b.log("Rewriting the config file")
                b.update_configuration()
            return
    
        if args.command == 'repopulate':
            b.repopulate()
            return 
    
        if hasattr(args,'clean') and args.clean:
            # If the clean arg is set, then we need to run  clean, and all of the
            # earlerier build phases. 
            ph = {
                  'meta': ['clean'],
                  'prepare': ['clean'],
                  'build' : ['clean', 'prepare'],
                  'update' : ['clean', 'prepare'],
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
          
            return f(*args.args)
           
        
    
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

        if 'update' in phases:
                
            if b.pre_update():
                b.log("---- Update ---")
                if b.update():
                    b.post_update()
                    b.log("---- Done Updating ---")
                else:
                    b.log("---- Update exited with failure ---")
                    return False
            else:
                b.log("---- Skipping Update ---- ")

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

        
class BundleConfig(object):
   
    def __init__(self):
        pass


class BundleFileConfig(BundleConfig):
    '''Bundle configuration from a bundle.yaml file '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, root_dir):
        '''Load the bundle.yaml file and create a config object
        
        If the 'id' value is not set in the yaml file, it will be created and the
        file will be re-written
        '''

        super(BundleFileConfig, self).__init__()
        
        self.root_dir = root_dir
        self.local_file = os.path.join(self.root_dir,'bundle.yaml')
        
        self._run_config = get_runconfig(self.local_file)
        
        # If there is no id field, create it immediately and
        # write the configuration back out. 
   
        if not self._run_config.identity.get('id',False):
            from databundles.identity import DatasetNumber
            self._run_config.identity.id = str(DatasetNumber())
            self.rewrite()
   
        if not os.path.exists(self.local_file):
            raise ConfigurationError("Can't find bundle config file: ")

        
    @property
    def config(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
        
        return self._run_config

    @property
    def path(self):
        return os.path.join(self.cache, BundleFileConfig.BUNDLE_CONFIG_FILE)

    def rewrite(self, **kwargs):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time. Will also look for a config directory and copy the
        contents of files there into the bundle.yaml file, ad a key derived from the name
        of the file. '''
        import yaml
        from databundles.dbexceptions import ConfigurationError
        
        temp = self.local_file+".temp"
        old = self.local_file+".old"
        
        config = AttrDict()
    
        config.update_yaml(self.local_file)
        
        for k,v in kwargs.items():
            config[k] = v

   
        with open(temp, 'w') as f:
            config.dump(f)
    
        if os.path.exists(temp):
            os.rename(self.local_file, old)
            os.rename(temp,self.local_file )
            
    def dump(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time'''
        import yaml
        
        return yaml.dump(self._run_config, indent=4, default_flow_style=False)
   
   
    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        return self._run_config.group(group)

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self._run_config.group(name)

from databundles.run import AttrDict
class BundleDbConfigDict(AttrDict):
    
    _parent_key = None
    _bundle = None
    
    def __init__(self, bundle):

        super(BundleDbConfigDict, self).__init__()
    
        '''load all of the values'''
        from databundles.orm import Config as SAConfig
        
        for k,v in self.items():
            del self[k]
        
        s = bundle.database.session
        # Load the dataset
        self['identity'] = {}
        for k,v in bundle.dataset.to_dict().items():
            self['identity'][k] = v
            
        for row in s.query(SAConfig).all():
            if row.group not in self:
                self[row.group] = {}
                
            self[row.group][row.key] = row.value
            

    
class BundleDbConfig(BundleConfig):
    ''' Retrieves configuration from the database, rather than the .yaml file. '''

    database = None
    dataset = None

    def __init__(self, database):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        super(BundleDbConfig, self).__init__()
        
        if not database:
            raise Exception("Didn't get database")
        
        self.database = database
        self.dataset = self.get_dataset()
       
    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
      
        return {'identity':self.dataset.to_dict()}

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        
        return self.group(group)


    def group(self, group):
        '''return a dict for a group of configuration items.'''
        
        bd = BundleDbConfigDict(self)
      
        group = bd.get(group)
        
        if not group:
            return None
        
        
        return group

    def set_value(self, group, key, value):
        from databundles.orm import Config as SAConfig
        
        if self.group == 'identity':
            raise ValueError("Can't set identity group from this interface. Use the dataset")
        
        s = self.database.session
     
        key = key.strip('_')
  
        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == self.dataset.vid).delete()
        

        o = SAConfig(group=group, key=key,d_vid=self.dataset.vid,value = value)
        s.add(o)
        s.commit()       

    def get_value(self, group, key):
        
        group = self.group(group)
        
        if not group:
            return None
        
        return group.__getattr__(key)

    def get_dataset(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        from databundles.orm import Dataset
        from sqlalchemy.exc import DatabaseError
 
        s = self.database.session

        try:
            return  (s.query(Dataset).one())
        except Exception as e:
            raise Exception("ERROR: BundleDbConfig.get_dataset() got bad database: '{}'; {}".format(self.database.path, e))

    @property
    def partition(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Partition
 
        s = self.database.session

        return  (s.query(Partition).first())
   
   
if __name__ == '__main__':
    import databundles.run
    import sys
    databundles.run.run(sys.argv[1:], Bundle)  

    