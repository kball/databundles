"""The Bundle object is the root object for a bundle, which includes acessors 
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


 
from .filesystem import  BundleFilesystem
from .schema import Schema
from .partitions import Partitions

import os.path
from .dbexceptions import  ConfigurationError, ProcessError
from .run import get_runconfig

def get_identity(path):
    '''Get an identity from a database, either a bundle or partition'''
    from .database.sqlite import SqliteBundleDatabase #@UnresolvedImport
    
    raise Exception("Function deprecated")
    
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
        self._dataset_id = None # Needed in LibraryDbBundle to  disambiguate multiple datasets
        
        if not logger:
            from .util import get_logger
            self.logger = get_logger(__name__)
        else:
            self.logger = logger 
            
        import logging
        self.logger.setLevel(logging.INFO) 
        
        # This bit of wackiness allows the var(self.run_args) code
        # to work when there have been no artgs parsed. 
        class null_args(object):
            none = None
            multi = False
            test = False

        self.run_args = null_args()
        
        
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
        from repository import Repository #@UnresolvedImport

        if not self._repository:
            repo_name = 'default'
            self._repository =  Repository(self, repo_name)
            
        return self._repository
    
    @property
    def identity(self):
        '''Return an identity object. '''
        from .identity import Identity, Name, ObjectNumber

        if not self._identity: 

            self._identity =  Identity.from_dict(self.config.identity)
            
        return self._identity            

    def get_dataset(self, session):
        '''Return the dataset
        '''
        from sqlalchemy.orm.exc import NoResultFound
        
        from databundles.orm import Dataset

        try:
            if self._dataset_id:
                try:
                    return (session.query(Dataset).filter(Dataset.vid == self._dataset_id).one())
                except NoResultFound:
                    from dbexceptions import NotFoundError
                    raise NotFoundError("Failed to find dataset for id {} in {} "
                                        .format(self._dataset_id, self.database.dsn))
        
            else:
                
                return (session.query(Dataset).one())
        except:
            raise

    @property
    def dataset(self):
        '''Return the dataset'''
        return self.get_dataset(self.database.session)

       
    def _dep_cb(self, library, key, name, resolved_bundle):
        '''A callback that is called when the library resolves a dependency.
        It stores the resolved dependency into the bundle database'''

        if resolved_bundle.partition:
            ident = resolved_bundle.partition.identity
        else:
            ident = resolved_bundle.identity
    
        if not self.database.is_empty():
            with self.session:
                self.db_config.set_value('rdep', key, ident.to_dict())

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
        l.dep_cb = self._dep_cb
        
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
     
    def warn(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.warn(message)
        
    def fatal(self, message, **kwargs):
        '''Log a fata messsage and exit'''
        import sys 
        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from dbexceptions import FatalError
            raise FatalError(message)
    
class DbBundle(Bundle):

    def __init__(self, database_file, logger=None):
        '''Initialize a db and all of its sub-components. 
        
        If it does not exist, creates the db database and initializes the
        Dataset record and Config records from the db.yaml file. Through the
        config object, will trigger a re-load of the db.yaml file if it
        has changed. 
        
        Order of operations is:
            Create db.db if it does not exist
        '''
        from .database.sqlite import SqliteBundleDatabase #@UnresolvedImport

        super(DbBundle, self).__init__(logger=logger)
       
        self.database_file = database_file

        self.database = SqliteBundleDatabase(self, database_file)

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. 

        
    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''

        return os.path.join(self.path, *args) 
        
    def table_data(self, query):
        '''Return a petl container for a data table'''
        import petl 
        query = query.strip().lower()
        
        if 'select' not in query:
            query = "select * from {} ".format(query)
 
        return petl.fromsqlite3(self.database.path, query) #@UndefinedVariable


class LibraryDbBundle(Bundle):
    '''A database bundle that is built in place from the data in a library '''

    def __init__(self, database, dataset_id, logger=None):
        '''Initialize a db and all of its sub-components. 

        '''

        super(LibraryDbBundle, self).__init__(logger=logger)
   
        self._dataset_id = dataset_id
        self.database = database

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. s
        
    @property
    def path(self):
        raise NotImplemented()
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        raise NotImplemented() 
        



class BuildBundle(Bundle):
    '''A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration '''

    META_COMPLETE_MARKER = '.meta_complete'
    SCHEMA_FILE = 'schema.csv'
    SCHEMA_REVISED_FILE = 'schema-revised.csv'
    SCHEMA_OLD_FILE = 'schema-old.csv'

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

        # Library for the bundle
        lib_dir = self.filesystem.path('lib')
        if os.path.exists(lib_dir):
            import sys
            sys.path.append(lib_dir)

        self._build_time = None
        self._update_time = None
       
        self.exit_on_fatal = True
       
    @property
    def build_dir(self):
        
        try:
            cache = self.filesystem.get_cache_by_name('build')
            return cache.cache_dir
        except KeyError:
            return  self.filesystem.path(self.filesystem.BUILD_DIR)
        
       
    @property
    def path(self):
        return os.path.join(self.build_dir, self.identity.path) 

    @property
    def db_path(self):
        return self.path+'.db'

    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        return os.path.join(self.build_dir, self.identity.path, *args) 

    @property
    def database(self):
        from .database.sqlite import BuildBundleDb #@UnresolvedImport

        if self._database is None:
            self._database  = BuildBundleDb(self, self.db_path)

        return self._database

    @property
    def session(self):
        return self.database.lock

    @property
    def has_session(self):
        return self.database.has_session

    @property
    def db_config(self):
        return BundleDbConfig(self, self.database)

    def update_configuration(self):

        # Re-writes the undle.yaml file, with updates to the identity and partitions
        # sections. 

        self.config.rewrite(
                         identity=self.identity.dict,
                         partitions=[p.identity.sname for p in self.partitions]
                         )
        
        # Reload some of the values from bundle.yaml into the database configuration

        if self.config.build.get('dependencies'):
            dbc = self.db_config
            for k,v in self.config.build.get('dependencies').items():
                dbc.set_value('odep', k, v)

           
        self.database.rewrite_dataset()
                
        
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

        # Remove partitions
        self.rm_rf(self.sub_path())
        # Remove the database
        
        if self.database.exists():
            self.database.delete()
        
        
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

    def init_log_rate(self, N=None, message='', print_rate=None):
        from util import init_log_rate as ilr
        
        return ilr(self.log, N=N, message=message, print_rate = print_rate)



    ### Prepare is run before building, part of the devel process.  

    def pre_meta(self):
        '''Skips the meta stage if the :class:.`META_COMPLETE_MARKER` file already exists'''

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

        self.log('---- Pre-Prepare ----')
        
        if self.config.build.get('requirements',False):
            from util.packages import install
            import sys
            import imp
            
            python_dir = self.config.config.python_dir()
            
            if not python_dir:

                raise ConfigurationError("Can't install requirements without a configuration item for filesystems.python")
            
            if not os.path.exists(python_dir):
                os.makedirs(python_dir)
            
                
            sys.path.append(python_dir)
            
            self.log("Installing required packages in {}".format(python_dir))
            
            for k,v in self.config.build.requirements.items():
                
                try:
                    imp.find_module(k)
                    self.log("Required package already installed: {}->{}".format(k,v))
                except ImportError:
                    self.log("Installing required package: {}->{}".format(k,v))
                    install(python_dir,k,v)

        
        if self.is_prepared:
            self.log("Bundle has already been prepared")
            #raise ProcessError("Bundle has already been prepared")
     
            return False
        
        try:
            b = self.library.get(self.identity.id_)
            
            if b and b.identity.revision >= self.identity.revision:
                self.fatal(("Can't build this version. Library has version {} "
                            " which is greater than or equal this version {}")
                           .format(b.identity.revision, self.identity.revision))
                return False
            
        except Exception as e:
            self.error("Error in consulting library: {}".format(e.message))
        

        return True

    def prepare(self):
        from dbexceptions import NotFoundError
        
        # with self.session: # This will create the database if it doesn't exist, but it will be empty
        if not self.database.exists():
            self.log("Creating bundle database")
            self.database.create()
        else:
            self.log("Bundle database already exists")

        try:
            self.library.check_dependencies()
        except NotFoundError as e:
            self.fatal(e.message)

        if self.run_args and vars(self.run_args).get('rebuild',False):
            with self.session:
                self.rebuild_schema()
        else:
            
            sf  = self.filesystem.path(self.config.build.get('schema_file', 'meta/'+self.SCHEMA_FILE))
            if os.path.exists(sf):
                with open(sf, 'rbU') as f:
                    self.log("Loading schema from file: {}".format(sf))
                    self.schema.clean()
                    with self.session:
                        warnings,errors = self.schema.schema_from_file(f)
                    
                    for title, s,f  in (("Errors", errors, self.error), ("Warnings", warnings, self.warn)):
                        if s:
                            self.log("----- Schema {} ".format(title))
                            for table_name, column_name, message in s:
                                f("{:20s} {}".format("{}.{}".format(table_name if table_name else '', column_name if column_name else ''), message ))
                
                    if errors:
                        self.fatal("Schema load filed. Exiting") 
            else:
                self.log("No schema file ('{}') not loading schema".format(sf))   

        return True
    
    def rebuild_schema(self):
        sf  = self.filesystem.path(self.config.build.get('schema_file', 'meta/schema.csv'))
        with open(sf, 'rbU') as f:

            partitions = [p.identity for p in self.partitions.all]
            self.schema.clean()
            self.schema.schema_from_file(f)  
            
            for p in partitions:
                self.partitions.new_db_partition(p)

    
    def _revise_schema(self):
        '''Write the schema from the database back to a file. If the schema template exists, overwrite the
        main schema file. If it does not exist, use the revised file

        
        '''

        self.update_configuration()

        sf_out = self.filesystem.path('meta',self.SCHEMA_REVISED_FILE)

        # Need to expire the unmanaged cache, or the regeneration of the schema may 
        # use the cached schema object rather than the ones we just updated, if the schem objects
        # have alread been loaded. 
        self.database.unmanaged_session.expire_all()

        with open(sf_out, 'w') as f:
            self.schema.as_csv(f)    
                    
    def post_prepare(self):
        '''Set a marker in the database that it is already prepared. '''
        from datetime import datetime

        with self.session:
            self.db_config.set_value('process','prepared',datetime.now().isoformat())

            self._revise_schema()
                    
        return True

    @property
    def is_prepared(self):
        return ( self.database.exists() 
                 and not vars(self.run_args).get('rebuild',False) 
                 and  self.db_config.get_value('process','prepared', False))
   
    ### Build the final package

    def pre_build(self):
        from time import time
        import sys
        
        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")
        
        with self.session:
            if not self.db_config.get_value('process','prepared', False):
                raise ProcessError("Build called before prepare completed")
            
            self._build_time = time()
        
        python_dir = self.config.config.python_dir()
        
        if  python_dir and python_dir not in sys.path:
            sys.path.append(python_dir)
        

        return True
        
    def build(self):
        return False
    
    
    def post_build(self):
        '''After the build, update the configuration with the time required for the build, 
        then save the schema back to the tables, if it was revised during the build.  '''
        from datetime import datetime
        from time import time
        import shutil
        
          
        with self.session:
            self.db_config.set_value('process', 'built', datetime.now().isoformat())
            self.db_config.set_value('process', 'buildtime',time()-self._build_time)
            self.update_configuration()

            self._revise_schema()
        
        
        # Some original import files don't have a schema, particularly 
        # imported Shapefiles
        if os.path.exists(self.filesystem.path('meta',self.SCHEMA_FILE)):
            shutil.copy(
                        self.filesystem.path('meta',self.SCHEMA_FILE),
                        self.filesystem.path('meta',self.SCHEMA_OLD_FILE)
                        )
    
            shutil.copy(
                        self.filesystem.path('meta',self.SCHEMA_REVISED_FILE),
                        self.filesystem.path('meta',self.SCHEMA_FILE)
                        )
        

        self.post_build_write_stats()

    
        return True
    
    def post_build_write_stats(self):
        from sqlalchemy.exc import OperationalError
        
        # Create stat entries for all of the partitions. 
        for p in self.partitions:
            try:
                self.log("Writting stats for: {}".format(p.identity.name))
                p.write_stats()
            except NotImplementedError:
                self.log("Can't write stats (unimplemented) for partition: {}".format(p.identity.name))
            except ConfigurationError as e:
                self.error(e.message)
            except OperationalError as e:
                self.error("Failed to write stats for partition {}: {}".format(p.identity.name, e.message))
                raise
                    
    
    @property
    def is_built(self):
        '''Return True is the bundle has been built'''
        
        if not self.database.exists():
            return False

        v = self.db_config.get_value('process','built', False)
        
        return bool(v)
        
        
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
        with self.session:
            self.db_config.set_value('process', 'updated', datetime.now().isoformat())
            self.db_config.set_value('process', 'updatetime',time()-self._update_time)
            self.update_configuration()
        return True
        
    ### Submit the package to the library
 
    def pre_install(self):
        
        with self.session:
            self.update_configuration()
        
        return True
    
    def install(self, library_name=None, delete=False,  force=False):  
        '''Install the bundle and all partitions in the default library'''
     
        import databundles.library

        force = vars(self.run_args).get('force', force)

        with self.session:
            library_name = vars(self.run_args).get('library', 'default') if library_name is None else 'default'
            library_name = library_name if library_name else 'default'
    
            library = databundles.library.new_library(self.config.config.library(library_name), reset=True)
         
            self.log("{} Install to  library {}".format(self.identity.name, library_name))  
            dest = library.put(self, force=force)
            self.log("{} Installed".format(dest[1]))
            
            skips = self.config.group('build').get('skipinstall',[])
            
            for partition in self.partitions:
                
                if not os.path.exists(partition.database.path):
                    self.log("{} File does not exist, skipping".format(partition.database.path))
                    continue
                
                if partition.name in skips:
                    self.log('{} Skipping'.format(partition.name))
                else:
                    self.log("{} Install".format(partition.name))  
                    dest = library.put(partition, force=force)
                    self.log("{} Installed".format(dest[1]))
                    if delete:
                        os.remove(partition.database.path)
                        self.log("{} Deleted".format(partition.database.path))
                    

        return True
        
    def post_install(self):
        from datetime import datetime
        self.db_config.set_value('process', 'installed', datetime.now().isoformat())
        return True
    
    ### Submit the package to the repository
 
    def pre_submit(self):
        with self.session:
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

        self.log('---- Repopulate ----')
        
        b = self.library.get(self.identity.name)
        
        self.log('Copy bundle from {} to {} '.format(b.database.path, self.database.path))
        
        if not os.path.isdir(os.path.dirname(self.database.path)):
            os.makedirs(os.path.dirname(self.database.path))
            
        shutil.copy(b.database.path, self.database.path)
         
        # Restart with the new bundle database.
        newb = BuildBundle(self.bundle_dir)
        
        for newp in newb.partitions:
            self.log('Copy partition: {}'.format(newp.identity.name))

            b = self.library.get(newp.identity.vname)
            
            dir_ = os.path.dirname(newp.database.path);
            
            if not os.path.isdir(dir_):
                os.makedirs(dir_)
            
            shutil.copy(b.partition.database.path, newp.database.path)

    def parse_args(self,argv):

        self.run_args = self.args_parser.parse_args(argv)
 
        return self.run_args
    

    def run_build(self):
        b = self
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
            
        return True 


    def run_mp(self, method, arg_sets):
        from run import mp_run
        from multiprocessing import Pool, cpu_count
        
        n = int(self.run_args.multi)

        if n == 0:
            n = cpu_count()
        
        
        pool = Pool(n)

        pool.map(mp_run,[ (self.bundle_dir, method.__name__, args) 
                         for args in arg_sets])

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
            self.init_dataset_number()

   
        if not os.path.exists(self.local_file):
            raise ConfigurationError("Can't find bundle config file: ")

    def init_dataset_number(self):
        from databundles.identity import Identity, DatasetNumber, NumberServer

        try:
            ns = NumberServer(**self._run_config.group('numbers'))
            ds = ns.next()
        except Exception as e:
            self.error("Failed to get number from number sever; using self assigned: {}"
                .format(e.message))
            ds = DatasetNumber()

        ident = Identity.from_dict(self._run_config.identity)

        ident._on = ds.rev(self._run_config.identity.revision)

        d =  ident.dict
        d['creator'] = 'eric@sandiegodata.org'
        d['fqname'] = ident.fqname

        self.rewrite(**dict(identity=d))
        self._run_config = get_runconfig(self.local_file)

        import sys
        sys.exit(1)


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
        contents of files there into the bundle.yaml file, adding a key derived from the name
        of the file. '''

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

    def __init__(self, parent):

        super(BundleDbConfigDict, self).__init__()
    
        '''load all of the values'''
        from databundles.orm import Config as SAConfig
        
        for k,v in self.items():
            del self[k]

        # Load the dataset
        self['identity'] = {}
        for k,v in parent.dataset.dict.items():
            self['identity'][k] = v
            
        for row in parent.database.session.query(SAConfig).all():
            if row.group not in self:
                self[row.group] = {}
                
            self[row.group][row.key] = row.value

class BundleDbConfig(BundleConfig):
    ''' Retrieves configuration from the database, rather than the .yaml file. '''

    database = None
    dataset = None

    def __init__(self, bundle, database):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        super(BundleDbConfig, self).__init__()
        
        if not database:
            raise Exception("Didn't get database")
        
        self.bundle = bundle
        self.database = database

        self.dataset = bundle.dataset # (self.database.session.query(Dataset).one())
       
    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
        from databundles.orm import Config
        from collections import defaultdict
        
        d = defaultdict(dict)
      
        for cfg in self.database.session.query(Config).all():
           
            d[cfg.group][cfg.key] = cfg.value
      
        return d

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
        
            key = key.strip('_')
      
        self.database.session.query(SAConfig).filter(SAConfig.group == group,
                                  SAConfig.key == key,
                                  SAConfig.d_vid == self.dataset.vid).delete()
         
 
        o = SAConfig(group=group, key=key,d_vid=self.dataset.vid,value = value)
        self.database.session.add(o)

    def get_value(self, group, key, default=None):
        
        group = self.group(group)
        
        if not group:
            return None
        
        try:
            return group.__getattr__(key)
        except KeyError:
            if default is not None:
                return default
            raise


    @property
    def partition(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Partition

        return  (self.database.session.query(Partition).first())
   
   
#if __name__ == '__main__':
#    import databundles.run
#    import sys
#    databundles.run.run(sys.argv[1:], Bundle)  

    