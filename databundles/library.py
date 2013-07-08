"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from databundles.run import  get_runconfig

import os.path
from databundles.util import temp_file_name
from databundles.dbexceptions import ConfigurationError, NotFoundError
from databundles.filesystem import  Filesystem
from  databundles.identity import new_identity
from databundles.bundle import DbBundle
        
import databundles

from collections import namedtuple
from sqlalchemy.exc import IntegrityError

import Queue

libraries = {}

# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger. 
import logging #@UnusedImport
import logging.handlers #@UnusedImport

class NullHandler(logging.Handler):
    def emit(self, record):
        pass
    
import threading
import time
class DumperThread (threading.Thread):
    """Run a thread for a library to try to dump the database to the retome at regular intervals"""
    
    lock = threading.Lock()
    
    def __init__(self,library):
        
        self.library = library
        threading.Thread.__init__(self)
        #self.daemon = True
        self.library.logger.setLevel(logging.DEBUG)
        self.library.logger.debug("Initialized Dumper")

    def run (self):

        self.library.logger.debug("Run Dumper")
        
        if not self.library.remote:
            self.library.logger.debug("No remote")
            return
        
        with DumperThread.lock:

            time.sleep(5)

            backed_up = self.library.backup()
    
            if backed_up:
                self.library.logger.debug("Backed up database")
            else:
                self.library.logger.debug("No backup")


def get_database(config=None,name='library'):
    """Return a new `LibraryDb`, constructed from a configuration
    
    :param config: a `RunConfig` object
    :rtype: a `LibraryDb` object
    
    If config is None, the function will constuct a new RunConfig() with a default
    constructor. 
    
    """
    import tempfile 
    
    if config is None:
        config = get_runconfig()    

    if not config.library:
        raise ConfigurationError("Didn't get library configuration value")
    
    root_dir = config.filesystem.get('root_dir',tempfile.gettempdir())
    db_config = config.database.get(name)
    
    db_config.dbname = db_config.dbname.format(root=root_dir)
    
    if not db_config:
        raise ConfigurationError("Didn't get database.{} configuration value".format(name))
    
    database = LibraryDb(**db_config)      
    database.create() # creates if does not exist. 
    
    return database


def _get_library(config=None, name='default'):
    from databundles.filesystem import Filesystem
    
    if name is None:
        name = 'default'
    
    if config is None:
        config = get_runconfig()
    
    sc = config.library.get(name,False)
    

    if not sc:
        raise Exception("Failed to get library.{} config key ".format(name))
    
    filesystem = Filesystem(config)
    cache = filesystem.get_cache(sc.filesystem, config)
    
    database = get_database(config, name=sc.database)
    
    remote_name = sc.get('remote',None)

    if remote_name:
        from  databundles.client.rest import Rest
        if not isinstance(remote_name, basestring):
            raise Exception("Deprecated")
        elif remote_name.startswith('http'):
            # It is a URL, and it points to an api that wil be used directly. 
            
            url = remote_name
            remote =  Rest(url, config.group('accounts'))
        else:
            # It is a name of a filesystem configuration
            
            remote = Filesystem._get_cache(config.filesystem, remote_name )
    else:
        remote = None

    require_upload = sc.get('require-upload', False)

    l =  Library(cache = cache, 
                 database = database, 
                 remote = remote, 
                 require_upload = require_upload,
                 host = sc.get('host','localhost'),
                 port = sc.get('port',80)
                 )
    
    return l
    
def get_library(config=None, name='default', reset=False):
    """Return a new :class:`~databundles.library.Library`, constructed from a configuration
    
    :param config: a :class:`~databundles.run.RunConfig` object
    :rtype:  :class:`~databundles.library.Library` 
    
    If ``config`` is None, the function will constuct a new :class:`~databundles.run.RunConfig` with a default
    constructor. 
    
    """    

    global libraries
    
    if reset:
        libraries = {}
    
    if name is None:
        name = 'default'

    if name not in libraries:
  
        libraries[name] = _get_library(config, name)
    
    return libraries[name]

def copy_stream_to_file(stream, file_path):
    '''Copy an open file-list object to a file
    
    :param stream: stream to copy from 
    :param file_path: file to write to. Will be opened mode 'w'
    
    '''

    with open(file_path,'w') as f:
        chunksize = 8192
        chunk =  stream.read(chunksize) #@UndefinedVariable
        while chunk:
            f.write(chunk)
            chunk =  stream.read(chunksize) #@UndefinedVariable

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''

    Dbci = namedtuple('Dbc', 'dsn_template sql') #Database connection information 
   
    DBCI = {
            'postgres':Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}/{name}',sql='support/configuration-pg.sql'), # Stored in the databundles module. 
            'sqlite':Dbci(dsn_template='sqlite:///{name}',sql='support/configuration-sqlite.sql')
            }
    
    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
   
        self.dsn_template = self.DBCI[self.driver].dsn_template
        self.dsn = None
        self.sql = self.DBCI[self.driver].sql
        
        self._session = None
        self._engine = None
        self._connection  = None
                
        self.logger = databundles.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO) 
        
    def clone(self):
        return self.__class__(self.driver, self.server, self.dbname, self.username, self.password)
        
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
          
            self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                            server=self.server, name=self.dbname)

            self._engine = create_engine(self.dsn, echo=False) 
            
            from sqlalchemy import event
            event.listen(self._engine, 'connect', _pragma_on_connect)
            
        return self._engine

    @property
    def connection(self):
        '''Return an SqlAlchemy connection'''
        if not self._connection:
            self._connection = self.engine.connect()
            
        return self._connection

    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''
        
        from sqlalchemy import MetaData   
        metadata = MetaData(bind=self.engine)

        metadata.reflect(self.engine)

        return metadata

    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)

    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        
        if not self._session:    
            self.Session = sessionmaker(bind=self.engine)
            self._session = self.Session()
            
        return self._session
   
    def set_config_value(self, group, key, value):
        '''Set a configuration value in the database'''
        from databundles.orm import Config as SAConfig

        s = self.session

        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == 'none').delete()
        
        o = SAConfig(group=group,
                     key=key,d_vid='none',value = value)
        s.add(o)
        s.commit()  
   
    def get_config_value(self, group, key):
        
        from databundles.orm import Config as SAConfig

        s = self.session        
        
        try:
            c = s.query(SAConfig).filter(SAConfig.group == group,
                                     SAConfig.key == key,
                                     SAConfig.d_vid == 'none').first()
       
            return c
        except:
            return None
   
    @property
    def config_values(self):
        
        from databundles.orm import Config as SAConfig

        s = self.session        
        
        d = {}
        
        for config in s.query(SAConfig).filter(SAConfig.d_vid == 'none').all():
            d[(str(config.group),str(config.key))] = config.value
            
        return d
        
   
    def _mark_update(self):
        from databundles.orm import Config
        
        import datetime
        
        self.set_config_value('activity','change', datetime.datetime.utcnow().isoformat())
        
   
    def close(self):

        if self._session:    
            self._session.bind.dispose()
            self.Session.close_all()
            self.engine.dispose() 
            self._session = None
            self._engine = None
            
   
    def commit(self):
        self.session.commit()     
        
    def exists(self):
        
        self.engine
        
        if self.driver == 'sqlite':
            return os.path.exists(self.dbname)
        else :
            return True; # Don't know how to check for a postgres database. 
        
    
    def clean(self):
        s = self.session
        from databundles.orm import Column, Partition, Table, Dataset, Config, File
        
        s.query(Column).delete()
        s.query(Partition).delete()
        s.query(Table).delete()
        s.query(Dataset).delete()
        s.query(Config).delete()
        s.query(File).delete()
        s.commit()
 
        
    def create(self):
        
        """Create the database from the base SQL"""
        if not self.exists():    
         
            try:   
                script_str = os.path.join(os.path.dirname(databundles.__file__),
                                          self.PROTO_SQL_FILE)
            except:
                # Not sure where to find pkg_resources, so this will probably
                # fail. 
                from pkg_resources import resource_string #@UnresolvedImport
                
                script_str = resource_string(databundles.__name__, self.sql)
            
            self.load_sql(script_str)
            
            return True
        
        return False
    
    def _drop(self, s):
        s.execute("DROP TABLE IF EXISTS files")
        s.execute("DROP TABLE IF EXISTS columns")
        s.execute("DROP TABLE IF EXISTS partitions")
        s.execute("DROP TABLE IF EXISTS tables")
        s.execute("DROP TABLE IF EXISTS config")
        s.execute("DROP TABLE IF EXISTS datasets")
    
    def drop(self):
        s = self.session
        self._drop(s)
        s.commit()


    def load_sql(self, sql_text):
        
        #conn = self.engine.connect()
        #conn.close()
        
        if self.driver == 'postgres':
            import psycopg2 #@UnresolvedImport
            
            dsn = ("host={} dbname={} user={} password={} "
                    .format(self.server, self.dbname, self.username, self.password))
           
            conn = psycopg2.connect(dsn)
         
            cur = conn.cursor()
          
            self._drop(cur)
            
            cur.execute("COMMIT")
            cur.execute(sql_text) 
            cur.execute("COMMIT")
            
            conn.close()
        elif self.driver == 'sqlite':
            
            import sqlite3
            
            dir_ = os.path.dirname(self.dbname)
            if not os.path.exists(dir_):
                try:
                    os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
                except Exception as e: #@UnusedVariable
                    pass
                
                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory "+dir_)
            
            try:
                conn = sqlite3.connect(self.dbname)
            except:
                self.logger.error("Failed to open Sqlite database: {}".format(self.dbname))
                raise
                
            self._drop(conn)

            conn.commit()
            conn.executescript(sql_text)  
        
            conn.commit()

        else:
            raise RuntimeError("Unknown database driver: {} ".format(self.driver))

    def install_bundle_file(self, identity, bundle_file):
        """Install a bundle in the database, starting from a file that may
        be a partition or a bundle"""

        if isinstance(identity , dict):
            identity = new_identity(identity)
            
        if identity.is_bundle:
            bundle = DbBundle(bundle_file)
            
            self.install_bundle(bundle)
        
        
    def install_bundle(self, bundle):
        '''Copy the schema and partitions lists into the library database
        
        '''
        from databundles.orm import Dataset, Config
        from databundles.bundle import Bundle
           
        if not isinstance(bundle, Bundle):
            raise ValueError("Can only install a  Bundle object")

            # The Tables only get installed when the dataset is installed, 
            # not for the partition
            
        self._mark_update()
                
        #self.remove_bundle(bundle)
                
        # There should be only one dataset record in the 
        # bundle
        bdbs = bundle.database.session 
        s = self.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)
 
        for config in bdbs.query(Config).all():
            s.merge(config)
            
        s.commit()
        
 
        for table in dataset.tables:
            try:
                s.merge(table)
                s.commit()
            except IntegrityError as e:
                self.logger.error("Failed to merge table "+str(table.id_)+":"+ str(e))
                s.rollback()
                raise e
         
            for column in table.columns:
                try:
                    s.merge(column)
                    s.commit()
                except IntegrityError as e:
                    self.logger.error("Failed to merge column "+str(column.id_)+":"+ str(e) )
                    s.rollback()
                    raise e

        for partition in dataset.partitions:
            try:
                s.merge(partition)
                s.commit()
            except IntegrityError as e:
                self.logger.error("Failed to merge partition "+str(partition.identity.id_)+":"+ str(e))
                s.rollback()
                raise e

        s.commit()
        
    def remove_bundle(self, bundle):
        '''remove a bundle from the database'''
        
        from databundles.orm import Dataset
        
        s = self.session
        
        try:
            dataset, partition = self.get(bundle.identity) #@UnusedVariable
        except AttributeError:
            dataset, partition = self.get(bundle) #@UnusedVariable
            
            
        if not dataset:
            return False

        dataset = s.query(Dataset).filter(Dataset.id_==dataset.identity.id_).one()

        # Can't use delete() on the query -- bulk delete queries do not 
        # trigger in-ython cascades!
        s.delete(dataset)
  
        s.commit()
        
      
    def get(self,bp_id):
        '''Get a bundle or partition
        
        Gets a bundle or Partition object, referenced by a string generated by
        DatasetNumber or PartitionNumber, or by an object that has a name
        or id_ field. 
        
        Args:
            bp_id (Bundle|Partition|str) Specifies a bundle or partition to
                fetch. bp_id may be:
                    An ObjectNumber string id for a partition or dataset
                    An ObjectNumber object
                    An Identity object, for a partition or bundle
                    Any object that has an 'identity' attribute that is an Identity object
                    
                
        Returns:
            Bundle|Partition
        
        '''

        from databundles.identity import ObjectNumber, PartitionNumber, Identity
        from databundles.orm import Dataset
        from databundles.orm import Partition
        import sqlalchemy.orm.exc 
        
        s = self.session
    
        if isinstance(bp_id, basestring):
            bp_id = ObjectNumber.parse(bp_id)

            
        elif isinstance(bp_id, ObjectNumber):
            pass
        elif isinstance(bp_id, Identity):
            if not bp_id.vid:
                raise Exception("Identity does not have an id_ defined")
            bp_id = ObjectNumber.parse(bp_id.vid)
            
        else:
            # hope that is has an identity field
            bp_id = ObjectNumber.parse(bp_id.identity.vid)

        dataset = None
        partition = None

        queries = []

        if isinstance(bp_id, PartitionNumber):
            queries.append((2, s.query(Dataset, Partition).join(Partition).filter(Partition.vid == str(bp_id))))
            queries.append((2, s.query(Dataset, Partition).join(Partition).filter(Partition.id_ == str(bp_id))))
            
        else:
            queries.append((1, s.query(Dataset).filter(Dataset.vid == str(bp_id))))
            queries.append((1, s.query(Dataset).filter(Dataset.id_ == str(bp_id))))
                
        for c, q in queries:
            q = q.order_by(Dataset.revision.desc())
            
            try:
                if c == 1:
                    dataset = q.first()
                else:
                    dataset, partition = q.first()
                
                if dataset:
                    break
                
            except sqlalchemy.orm.exc.NoResultFound as e: #@UnusedVariable
                pass

        return dataset, partition
        
    def find(self, query_command):
        '''Find a bundle or partition record by a QueryCommand or Identity
        
        Args:
            query_command. QueryCommand or Identity
            
        returns:
            A list of identities, either Identity, for datasets, or PartitionIdentity
            for partitions. 

            
        '''
      
        from databundles.orm import Dataset
        from databundles.orm import Partition
        from databundles.identity import Identity
        from databundles.orm import Table
        s = self.session
        has_partition = False
        
        if isinstance(query_command, Identity):
            raise NotImplementedError()
            out = []
            for d in self.queryByIdentity(query_command).all():
                id_ = d.identity
                d.path = os.path.join(self.cache,id_.cache_key)
                out.append(d)

        if len(query_command.partition) == 0:
            query = s.query(Dataset, Dataset.id_) # Dataset.id_ is included to ensure result is always a tuple
        else:
            query = s.query(Dataset, Partition, Dataset.id_)
        
        if len(query_command.identity) > 0:
            for k,v in query_command.identity.items():
                try:
                    query = query.filter( getattr(Dataset, k) == v )
                except AttributeError:
                    # Dataset doesn't have the attribute, so ignore it. 
                    pass
                
        
        if len(query_command.partition) > 0:     
            query = query.join(Partition)
            has_partition = True
            for k,v in query_command.partition.items():
                from sqlalchemy.sql import or_
                
                if k == 'any': 
                    continue # Just join the partition
                elif k == 'table':
                    # The 'table" value could be the table id
                    # or a table name
                    query = query.join(Table)
                    query = query.filter( or_(Partition.t_id  == v,
                                              Table.name == v))
                elif k == 'space':
                    query = query.filter( or_(Partition.space  == v))
                    
                else:
                    query = query.filter(  getattr(Partition, k) == v )
        
        if len(query_command.table) > 0:
            query = query.join(Table)
            for k,v in query_command.table.items():
                query = query.filter(  getattr(Table, k) == v )

        out = []

        query = query.order_by(Dataset.revision.desc())

        for r in query.all():
            if has_partition:
                out.append(r.Partition.identity)
            else:
                out.append(r.Dataset.identity)

            
        return out
        
    def queryByIdentity(self, identity):
        from databundles.orm import Dataset, Partition
        from databundles.identity import Identity
        from databundles.partition import PartitionIdentity
        from sqlalchemy import desc
        
        s = self.database.session
        
        # If it is a string, it is a name or a dataset id
        if isinstance(identity, str) or isinstance(identity, unicode) : 
            query = (s.query(Dataset)
                     .filter( (Dataset.id_==identity) | (Dataset.name==identity)) )
        elif isinstance(identity, PartitionIdentity):
            
            query = s.query(Dataset, Partition)
            
            for k,v in identity.to_dict().items():
                d = {}
              
                if k == 'revision':
                    v = int(v)
                    
                d[k] = v
         
            query = query.filter_by(**d)
                
        elif isinstance(identity, Identity):
            query = s.query(Dataset)
            
            for k,v in identity.to_dict().items():
                d = {}
                d[k] = v
                
            query = query.filter_by(**d)

           
        elif isinstance(identity, dict):
            query = s.query(Dataset)
            
            for k,v in identity.items():
                d = {}
                d[k] = v
                query = query.filter_by(**d)
      
        else:
            raise ValueError("Invalid type for identity")
    
        query.order_by(desc(Dataset.revision))
     
        return query

        
    def add_file(self,path, group, ref, state='new'):
        from databundles.orm import  File
        
        stat = os.stat(path)
      
        s = self.session

        s.query(File).filter(File.path == path).delete()
      
        file_ = File(path=path, 
                     group=group, 
                     ref=ref,
                     modified=stat.st_mtime, 
                     state = state,
                     size=stat.st_size)
    
        s.add(file_)
        s.commit()
        
        self._mark_update()
        

    def add_ticket(self, identity, ticket):
        from databundles.orm import  File
        import time
        path = identity.cache_key
        group = ticket
        ref = identity.id_
        state = 'ticketed'
                
        s = self.session
      
        s.query(File).filter(File.path == path and File.state == state).delete()
      
        file_ = File(path=path, 
                     group=group, 
                     ref=ref,
                     modified=time.time(),
                     state = state,
                     size=0)
    
        s.add(file_)
        s.commit()       
        
    def get_ticket(self, ticket):
        from databundles.orm import  File
        s = self.session
        
        return s.query(File).filter(File.group == ticket).one()
        

    def get_file_by_state(self, state):
        """Return all files in the database with the given state"""
        from databundles.orm import  File
        s = self.session
        if state == 'all':
            return s.query(File).all()
        else:
            return s.query(File).filter(File.state == state).all()

    def remove_file(self,path):
        pass
    
    def get_file(self,path):
        pass

    #
    # Database backup and restore. Synchronizes the database with 
    # a remote. This is used when a library is created attached to a remote, and 
    # needs to get the library database from the remote. 
    #
        
        
    def _copy_db(self, src, dst):
        
        for name, table in self.metadata.tables.items():
            rows = src.session.execute(table.select()).fetchall()
            for row in rows:
                dst.session.execute(table.insert(), row)
                        
            dst.session.commit()
            
                        
    def dump(self, path):
        '''Copy the database to a new Sqlite file, as a backup. '''
        import datetime

        dst = LibraryDb(driver='sqlite', dbname=path)

        dst.create()
        
        self.set_config_value('activity','dump', datetime.datetime.utcnow().isoformat())
        
        self._copy_db(self, dst)


    def needs_dump(self):
        '''Return true if the last dump date is after the last change date, and
        the last change date is more than 10s in the past'''
        import datetime
        from dateutil  import parser
        
        configs = self.config_values
        
        td = datetime.timedelta(seconds=10)
        
        changed =  parser.parse(configs.get(('activity','change'),datetime.datetime.fromtimestamp(0).isoformat()))
        dumped = parser.parse(configs.get(('activity','dump'),datetime.datetime.fromtimestamp(0).isoformat()))
        dumped_past = dumped + td
        now = datetime.datetime.utcnow()

        if ( changed > dumped and now > dumped_past): 
            return True
        else:
            return False
    
    def restore(self, path):
        '''Restore a sqlite database dump'''
        import datetime
        
        self.create()
        
        src = LibraryDb(driver='sqlite', dbname=path)
        
        self._copy_db(src, self)

        self.set_config_value('activity','restore', datetime.datetime.utcnow().isoformat())

        
class QueryCommand(object):
    '''An object that contains and transfers a query for a bundle
    
    Components of the query can include. 
    
    Identity
        id
        source
        dataset
        subset
        variation
        creator
        revision

    
    Column 
        name, altname
        description
        keywords
        datatype
        measure 
        units
        universe
    
    Table
        name, altname
        description
        keywords
    
    Partition
        time
        space
        table
        other
        
    When the Partition search is included, the other three components are used
    to find a bundle, then the pretition information is used to select a bundle

    All of the  values are text, except for revision, which is numeric. The text
    values are used in an SQL LIKE phtase, with '%' replaced by '*', so some 
    example values are: 
    
        word    Matches text field, that is, in it entirety, 'word'
        word*   Matches a text field that begins with 'word'
        *word   Matches a text fiels that
    
    '''

    def __init__(self, dict_ = None):
        
        if dict_ is None:
            dict_ = {}
        
        self._dict = dict_
    
    def to_dict(self):
        return self._dict
    
    def from_dict(self, dict_):
        for k,v in dict_.items():
            print "FROM DICT",k,v

    
    def getsubdict(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''

        if not group in self._dict:
            self._dict[group] = {}
            
        inner = self._dict[group]
        query = self

        return _qc_attrdict(inner, query)

    @property
    def identity(self):
        '''Return an array of terms for identity searches''' 
        return self.getsubdict('identity')
    
    @identity.setter
    def identity(self, value):
        self._dict['identity'] = value
    
    @property
    def table(self):
        '''Return an array of terms for table searches'''
        return self.getsubdict('table')
    
    @property
    def column(self):
        '''Return an array of terms for column searches'''
        return self.getsubdict('column')
    
    @property
    def partition(self):
        '''Return an array of terms for partition searches'''
        return self.getsubdict('partition')  
         

    def __str__(self):
        return str(self._dict)



class _qc_attrdict(object):
    

    def __init__(self, inner, query):
        self.__dict__['inner'] = inner
        self.__dict__['query'] = query
        
    def __setattr__(self, key, value):
        #key = key.strip('_')
        inner = self.__dict__['inner']
        inner[key] = value

    def __getattr__(self, key):
        #key = key.strip('_')
        inner = self.__dict__['inner']
        
        if key not in inner:
            return None
        
        return inner[key]
    
    def __len__(self):
        return len(self.inner)
    
    def __iter__(self):
        return iter(self.inner)
    
    def items(self):
        return self.inner.items()
    
    def __call__(self, **kwargs):
        for k,v in kwargs.items():
            self.inner[k] = v
        return self.query
            
class Library(object):
    '''
    
    '''
    import collections

    # Return value for get()
    Return = collections.namedtuple('Return',['bundle','partition'])
    
    # Return value for earches
    ReturnDs = collections.namedtuple('ReturnDs',['dataset','partition'])
    
    def __init__(self, cache,database, remote=None, sync=False, require_upload = False, host=None,port = None):
        '''
        Libraries are constructed on the root cache name for the library. 
        If the cache does not exist, it will be created. 
        
        Args:
        
            cache: a path name to a directory where bundle files will be stored
            database: 
            remote: URL of a remote library, for fallback for get and put. 
            sync: If true, put to remote synchronously. Defaults to False. 
   
        '''
 
        self.cache = cache
        self._database = database
        self._remote = remote
        self.sync = sync
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port

        self.require_upload = require_upload

        self.dependencies = None

        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = logging.getLogger(__name__)
        #self.logger.setLevel(logging.DEBUG)

        self.needs_update = False
    
    def clone(self):
        
        return self.__class__(self.cache, self.database.clone(), self._remote, self.sync, self.require_upload, self.host, self.port)
    
    @property
    def remote(self):
        if self._remote:
            return self._remote # When it is a URL to a REST interface. 
        else:
            return self.cache.remote
    
    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return self._database
  
    def _get_bundle_path_from_id(self, bp_id):
        
        try:
            # Assume it is an Identity, or Identity-like
            dataset, partition = self.database.get(bp_id.vid)
            
            return  dataset, partition
        except AttributeError:
            pass
        
        # A string, either a name or an id
        dataset, partition = self.database.get(bp_id)

        return dataset, partition
            
    def get_ref(self,bp_id):
        from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber, Identity, PartitionIdentity

        term = None

        if isinstance(bp_id, Identity):
            if bp_id.revision:
                if bp_id.vid:
                    term = bp_id.vid
                else:
                    term = bp_id.vname
            else:
                if bp_id.id_:
                    term = bp_id.id_
                else:
                    term = bp_id.name
        elif isinstance(bp_id, basestring):
            try:
                on = ObjectNumber.parse(bp_id)
                
                if not ( isinstance(on, DatasetNumber) or isinstance(on, PartitionNumber)):
                    raise ValueError("Object number must be for a Dataset or Partition: {} ".format(bp_id))
                
                term  = bp_id
            except: # Not parsable
                term  = bp_id # Possibly a name
            
        else:
            ValueError("Don't know how to get ref for '{}'".format(type(bp_id)))
                    
        # If dataset is not None, it means the file already is in the cache.
        dataset = None
        
        queries = []
        
        queries.append(QueryCommand().identity(name = term) )
        queries.append(QueryCommand().identity(vname = term) )
        queries.append(QueryCommand().identity(id_ = term) )
        queries.append(QueryCommand().identity(vid = term) )
                              
        queries.append(QueryCommand().partition(name = term))
        queries.append(QueryCommand().partition(vname = term))
        queries.append(QueryCommand().partition(id_ = term))
        queries.append(QueryCommand().partition(vid = term))
        
        
        for q in queries:
            r = self.find(q)
            
            if len(r) > 1:
                # Names aren't unique ( vnames are )  and they are ordered so the highest 
                # version is first, so return that one
                r = r.pop()
            elif len(r) == 0:
                r = None
            else:
                r = r.pop()            
            
    
            if r:
                dataset, partition  = self._get_bundle_path_from_id(r.vid)   
                break
    
                    
        # No luck so far, so now try to get it from the remote library
        if not dataset and self.remote:
            import socket
         
            try:
                r = self.remote.find(bp_id)

                if r:
                    r = r[0]
                    
                    if r.is_partition:
                        dataset = r.as_dataset
                        partition = r
                    else:
                        dataset = r
                        partition = None


            except socket.error:
                self.logger.error("Connection to remote ")
        elif dataset:
            from identity import new_identity
            dataset = Identity(**dataset.to_dict())
            partition = new_identity(partition.to_dict()) if partition else None
            
        if not dataset:
            return False, False
   
        return  dataset, partition

    def _get_remote_dataset(self, dataset):
        from databundles.identity import Identity
        
        try:# ORM Objects
            identity = Identity(**(dataset.to_dict()))
        except:# Tuples
            identity = Identity(**(dataset._asdict()))        

        r = self.remote.get(identity.id_)
        
        if not r:
            return False
        
        # Store it in the local cache. 
        
        if not self.cache.has(identity.cache_key):
            abs_path = self.cache.put(r, identity.cache_key )
        else:
            abs_path = self.cache.get(identity.cache_key )
        
        if not os.path.exists(abs_path):
            raise Exception("Didn't get file '{}' for id {}".format(abs_path, identity.cache_key))
            
        bundle = DbBundle(abs_path)
  
        # Ensure the file is in the local library. 

        self.database.add_file(abs_path, self.cache.repo_id, bundle.identity.id_, 'pulled')                 
        self.database.install_bundle(bundle)
      
        return abs_path
      
    def _get_remote_partition(self, bundle, partition):
        
        from databundles.identity import  PartitionIdentity, new_identity 

        identity = new_identity(partition.to_dict(), bundle=bundle) 

            

        p = bundle.partitions.get(identity.id_) # Get partition information from bundle
        
        if not p:
            from databundles.dbexceptions import NotFoundError
            raise NotFoundError("Failed to find partition {} in bundle {}"
                                .format(identity.name, bundle.identity.name))
        
        p_database_path = p.database.path
      
        r = self.remote.get_partition(bundle.identity.id_, p.identity.id_)
        # Store it in the local cache. 
        p_abs_path = self.cache.put(r,p.identity.cache_key)

        if os.path.realpath(p_database_path) != os.path.realpath(p_abs_path):
            m =( "Path mismatch in downloading partition: {} != {}"
                 .format(os.path.realpath(p_database_path),
                                os.path.realpath(p_abs_path)))
            
                              
            self.logger.error(m)
            raise Exception(m)

        # Ensure the file is in the local library. 
        self.database.add_file(p_abs_path, self.cache.repo_id, bundle.identity.id_, 'pulled')                 
    
        return p_abs_path, p
            
    def get(self,bp_id):
        '''Get a bundle, given an id string or a name '''

        # Get a reference to the dataset, partition and relative path
        # from the local database. 

        dataset, partition = self.get_ref(bp_id)

        if partition:
            return self._get_partition(dataset, partition)
        elif dataset:
            return self._get_dataset(dataset)  

    def _get_dataset(self, dataset):

        # Try to get the file from the cache. 
        abs_path = self.cache.get(dataset.cache_key)


        # Not in the cache, try to get it from the remote library, 
        # if a remote was set. 

        if not abs_path and self.remote:
            abs_path = self._get_remote_dataset(dataset)
            
        if not abs_path or not os.path.exists(abs_path):
            return False
       
        bundle = DbBundle(abs_path)
            
        bundle.library = self

        # For filesystems that have an upstream remote that is S3, it is possible
        # to get a dataset that isn't in the library, so well need to add it. 

        dataset, partition =  self.database.get(bundle.identity)

        if dataset is None:
            self._get_remote_dataset(bundle.identity) # Side effect of adding to the library 
            
        return bundle
    
    def _get_partition(self,  dataset, partition):
        from databundles.dbexceptions import NotFoundError
        
        r = self._get_dataset(dataset)

        if not r:
            return False

        p =  r.partitions.partition(partition)
        
        if not p:
            raise NotFoundError(" Partition '{}' not in bundle  '{}' "
                                .format(partition, r.identity.name ))
        
        rp = self.cache.get(p.identity.cache_key)
    
        if not os.path.exists(p.database.path):
            if self.remote:
                self._get_remote_partition(r,partition)
            else:
                raise NotFoundError("""Didn't get partition in {} for id {} {}. 
                                    Partition found, but path {} ({}?) not in local library and remote not set. """
                               .format(r.identity.name, p.identity.id_,p.identity.name,
                                       p.database.path, rp))
        p.library = self   

        r.partition = p

        return r

        
    def find(self, query_command):
        return self.database.find(query_command)
        
    def path(self, rel_path):
        """Return the cache path for a cache key"""
        
        return self.cache.path(rel_path)
        
        
    def add_dependency(self, key, name):
        
        if not self.dependencies:
            self.dependencies = {}
        
        self.dependencies[key] = name
        
    def _add_dependencies(self):

        if not self.bundle:
            raise ConfigurationError("Can't use the dep() method for a library that is not attached to a bundle");

        if not self.dependencies:
            self.dependencies = {}

        group = self.bundle.config.group('build')
        
        try:
            deps = group.get('dependencies')
        except AttributeError:
            deps = None
            
        if not deps:
            raise ConfigurationError("Configuration has no 'dependencies' group")
        
        for k,v in deps.items():
            self.dependencies[k] = v
                
    def dep(self,name):
        """"Bundle version of get(), which uses a key in the 
        bundles configuration group 'dependencies' to resolve to a name"""
        
        if not self.dependencies:
            self._add_dependencies()
        

        bundle_name = self.dependencies.get(name, False)
        
        if not bundle_name:
            raise ConfigurationError("No dependency names '{}'".format(name))
        
        b = self.get(bundle_name)
        
        if not b:
            self.bundle.error("Failed to get dependency,  key={}, id={}".format(name, bundle_name))
            raise NotFoundError("Failed to get dependency, key={}, id={}".format(name, bundle_name))
        
        return b

    def put_remote_ref(self,identity):
        '''Store a reference to a partition that has been uploaded directly to the remote'''
        pass
        
    def put_file(self, identity, file_path, state='new'):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity''' 
        
        if isinstance(identity , dict):
            identity = new_identity(identity)

        dst = self.cache.put(file_path,identity.cache_key)

        if not os.path.exists(dst):
            raise Exception("cache {}.put() didn't return an existent path. got: {}".format(type(self.cache), dst))

        if self.remote and self.sync:
            self.remote.put(identity, file_path)

        self.database.add_file(dst, self.cache.repo_id, identity.id_,  state)

        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)

        return dst, identity.cache_key, self.cache.public_url_f()(identity.cache_key)
     
    def put(self, bundle):
        '''Install a bundle or partition file into the library.
        
        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object
        
        '''
        from bundle import Bundle
        from partition import Partition
        
        if not isinstance(bundle, (Partition, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")
        
        # In the past, Partitions could be cloaked as Bundles. Disallow this 
        if isinstance(bundle, Bundle) and bundle.db_config.info.type == 'partition':
            raise RuntimeError("Don't allow partitions cloaked as bundles anymore ")
        
        bundle.identity.name # throw exception if not right type. 

        dst, cache_key, url = self.put_file(bundle.identity, bundle.database.path)

        return dst, cache_key, url

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.database.remove_bundle(bundle)
        
        self.cache.remove(bundle.identity.cache_key)
        
    def clean(self):
        self.database.clean()
        
    def purge(self):
        """Remove all records from the library database, then delete all
        files from the cache"""
        self.clean()
        self.cache.clean()
        


    @property
    def datasets(self):
        '''Return an array of all of the dataset records in the library database'''
        from databundles.orm import Dataset
       
        return [d for d in self.database.session.query(Dataset).all()]

  

  
    @property
    def new_files(self):
        '''Generator that returns files that should be pushed to the remote
        library'''
        
        new_files = self.database.get_file_by_state('new')
   
        for nf in new_files:
            yield nf

    def push(self, file_=None):
        """Push any files marked 'new' to the remote
        
        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files. 
        
        """
        
        if not self.remote:
            raise Exception("Can't push() without defining a remote. ")
 
        if file_ is not None:
            
            dataset, partition = self.database.get(file_.ref)
            
            if partition:
                identity = partition.identity
            else:
                identity = dataset.identity
            
            self.remote.put(file_.path, identity)
            file_.state = 'pushed'
            self.database.commit()
        else:
            for file_ in self.new_files:
                self.push(file_)
    
    #
    # Backup and restore
    #
    
    def run_dumper_thread(self):
        '''Run a thread that will check the database and call the callback when the database should be
        backed up after a change. '''
        
        dt = DumperThread(self.clone())
        dt.start()

        return dt
    
    def backup(self):
        '''Backup the database to the remote, but only if the database needs to be backed up. '''


        if not self.database.needs_dump():
            return False

        backup_file = temp_file_name()+".db"

        self.database.dump(backup_file)

        path = self.remote.put(backup_file,'_/library.db')

        os.remove(backup_file)   

        return path

    def can_restore(self):
        
        backup_file = self.cache.get('_/library.db')
        
        if backup_file:
            return True
        else:
            return False

    def restore(self):
        '''Restore the database from the remote'''

        # This requires that the cache have and upstream that is also the remote
        backup_file = self.cache.get('_/library.db')

        self.database.restore(backup_file)

        os.remove(backup_file)   

        return backup_file        

    def remote_rebuild(self):
        '''Rebuild the library from the contents of the remote'''

        self.clean()
        for rel_path in self.remote.list():
            path = self.cache.get(rel_path) # The cache and the remote must be connected!
     
            bundle = DbBundle(path)
            identity = bundle.identity
     
            self.database.add_file(path, self.cache.repo_id, identity.id_,  'pushed')

            self.database.install_bundle_file(identity, path)
            


    def rebuild(self):
        '''Rebuild the database from the bundles that are already installed
        in the repositry cache'''
  
        from databundles.bundle import DbBundle
   
        bundles = []
        for r,d,f in os.walk(self.cache.cache_dir): #@UnusedVariable
            for file_ in f:
                
                if file_.endswith(".db"):
                    try:
                        b = DbBundle(os.path.join(r,file_))
                        # This is a fragile hack -- there should be a flag in the database
                        # that diferentiates a partition from a bundle. 
                        f = os.path.splitext(file_)[0]
    
                        if b.db_config.get_value('info','type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.name, file_))
                            bundles.append(b)
                            
                    except Exception as e:
                        self.logger.error('Failed to process {} : {} '.format(file_, e))

        self.database.clean()
        
        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.name))
            self.database.install_bundle(bundle)
            
    
        
        self.database.commit()
        return bundles


def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    
    #dbapi_con.execute('PRAGMA foreign_keys = ON;')
    return # Not clear that there is a performance improvement. 
    dbapi_con.execute('PRAGMA journal_mode = MEMORY')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')
