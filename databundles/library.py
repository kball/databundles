"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path

import databundles
import databundles.util
from databundles.run import  get_runconfig #@UnresolvedImport
from databundles.util import temp_file_name
from databundles.dbexceptions import ConfigurationError, NotFoundError
from databundles.identity import new_identity
from databundles.bundle import DbBundle

from collections import namedtuple
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError

import Queue

ROOT_CONFIG_NAME = 'a0'
ROOT_CONFIG_NAME_V = 'a0/001'

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
                self.library.logger.debug("Did not back up database")



def _new_library(config):

    import copy
    
    config = copy.deepcopy(config)
    
   

    from cache import new_cache
    from cache import RemoteMarker

    #import pprint; pprint.pprint(config.to_dict())

    cache = new_cache(config['filesystem'])
    
    database = LibraryDb(**dict(config['database']))    
    
    database.create()
    
    remote = new_cache(config['remote']) if 'remote' in config else None

    
    config['name'] = config['_name'] if '_name' in config else 'NONE'

    for key in ['_name', 'filesystem', 'database', 'remote' ]:
        if key in config:
            del config[key]


    if remote and (not isinstance(remote, RemoteMarker) 
                   and not isinstance(remote.last_upstream(), RemoteMarker)):
        raise ConfigurationError("Library remote must hace a RemoteMarker interface: {}".format(config))

    # Idea for integrating the remote into the cache. 
    #lus = cache.last_upstream()
    #lus.upstream = remote
    #remote = remote.last_upstream

    l =  Library(cache = cache, 
                 database = database, 
                 remote = remote,
                 **config)

    return l

    
def new_library(config, reset=False):
    """Return a new :class:`~databundles.library.Library`, constructed from a configuration
    
    :param config: a :class:`~databundles.run.RunConfig` object
    :rtype:  :class:`~databundles.library.Library` 
    
    If ``config`` is None, the function will constuct a new :class:`~databundles.run.RunConfig` with a default
    constructor. 
    
    """    

    global libraries
    
    if reset:
        libraries = {}
    
    name = config['_name']
    
    if name is None:
        name = 'default'

    if name not in libraries:
        libraries[name] = _new_library(config)

    l =  libraries[name]
    l.clear_dependencies()
    
    return l


    
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
            'postgres':Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}',sql='support/configuration-pg.sql'), # Stored in the databundles module. 
            'sqlite':Dbci(dsn_template='sqlite:///{name}',sql='support/configuration-sqlite.sql'),
            'mysql':Dbci(dsn_template='mysql://{user}:{password}@{server}{colon_port}/{name}',sql='support/configuration-sqlite.sql')
            }
    
    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None, port=None, **kwargs):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
   
        if port:
            self.colon_port = ':'+str(port)
        else:
            self.colon_port = ''
   
        self.dsn_template = self.DBCI[self.driver].dsn_template
        self.dsn = None
  
        self._session = None
        self._engine = None
        self._connection  = None
           
                
        self.logger = databundles.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO) 
        
        self.enable_delete = False
        
    def __del__(self):
        pass # print  'closing LibraryDb'
        
    def clone(self):
        return self.__class__(self.driver, self.server, self.dbname, self.username, self.password)
        
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        from database.sqlite import _on_connect_update_schema
        from sqlalchemy.pool import AssertionPool
        
        if not self._engine:
            self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                            server=self.server, name=self.dbname, colon_port=self.colon_port)

            self._engine = create_engine(self.dsn,echo=False) 
            
            from sqlalchemy import event
            
            if self.driver == 'sqlite':
                event.listen(self._engine, 'connect', _pragma_on_connect)
                #event.listen(self._engine, 'connect', _on_connect_update_schema)
                _on_connect_update_schema(self.connection)

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

    def inserter(self,table_name, **kwargs):
        from database.inserter import ValueInserter
        from sqlalchemy.schema import Table
        
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
        
        return ValueInserter(None, table , self,**kwargs)

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
        from sqlalchemy.exc import IntegrityError, ProgrammingError
        
        s = self.session

        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == ROOT_CONFIG_NAME_V).delete()
                        

        try:
            o = SAConfig(group=group,key=key,d_vid=ROOT_CONFIG_NAME_V,value = value)
            s.add(o)
            s.commit()  
        except IntegrityError:
            s.rollback()
            o = s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == ROOT_CONFIG_NAME_V).one()
                      
            o.value = value           
            s.merge(o)
            s.commit()  

    def get_config_value(self, group, key):
        
        from databundles.orm import Config as SAConfig

        s = self.session        
        
        try:
            c = s.query(SAConfig).filter(SAConfig.group == group,
                                     SAConfig.key == key,
                                     SAConfig.d_vid == ROOT_CONFIG_NAME_V).first()
       
            return c
        except:
            return None
   
    @property
    def config_values(self):
        
        from databundles.orm import Config as SAConfig

        s = self.session        
        
        d = {}
        
        for config in s.query(SAConfig).filter(SAConfig.d_vid == ROOT_CONFIG_NAME_V).all():
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
        from databundles.orm import Dataset
        from sqlalchemy.exc import  ProgrammingError, OperationalError
        self.engine
        
        if self.driver == 'sqlite' and not os.path.exists(self.dbname):
                return False

        
        try: 
            try: rows = self.engine.execute("SELECT * FROM datasets WHERE d_vid = '{}' ".format(ROOT_CONFIG_NAME_V)).fetchone()
            except Exception as e:
                rows = False

            if not rows:
                return False
            else:
                return True
        except:
            raise
            return False

    
    def clean(self, add_config_root=True):
        s = self.session
        from databundles.orm import Column, Partition, Table, Dataset, Config, File
        
        s.query(Config).delete()
        s.query(File).delete()
        s.query(Column).delete()
        s.query(Partition).delete()
        s.query(Table).delete()
        s.query(Dataset).delete()

        if add_config_root:
            self._add_config_root()

        s.commit()
 
        
    def _creation_sql(self):
        try:   
            script_str = os.path.join(os.path.dirname(databundles.__file__),
                                      self.PROTO_SQL_FILE)
        except:
            # Not sure where to find pkg_resources, so this will probably
            # fail. 
            from pkg_resources import resource_string #@UnresolvedImport
            
            script_str = resource_string(databundles.__name__, self.sql)        
    
        return script_str
    
    def create(self):
        """Create the database from the base SQL"""

        
        if not self.exists():  
            self.enable_delete = True  
            self.create_tables()
            self._add_config_root()

            return True
        
        return False
    
    def _add_config_root(self):
        from databundles.orm import Dataset
        from sqlalchemy.orm.exc import NoResultFound 
        
        try: 
            self.session.query(Dataset).filter(Dataset.vid==ROOT_CONFIG_NAME).one()
        except NoResultFound:
            o = Dataset(
                        id=ROOT_CONFIG_NAME,
                        name=ROOT_CONFIG_NAME, 
                        vname=ROOT_CONFIG_NAME_V,
                        source=ROOT_CONFIG_NAME,
                        dataset = ROOT_CONFIG_NAME,
                        creator=ROOT_CONFIG_NAME,
                        revision=1,
                        )
            self.session.add(o)
            self.session.commit()  
             
    def _clean_config_root(self):
        '''Hack need to clean up some installed databases'''
        from databundles.orm import Dataset

        ds = self.session.query(Dataset).filter(Dataset.id_==ROOT_CONFIG_NAME).one()

        ds.id_=ROOT_CONFIG_NAME
        ds.name=ROOT_CONFIG_NAME
        ds.vname=ROOT_CONFIG_NAME_V
        ds.source=ROOT_CONFIG_NAME
        ds.dataset = ROOT_CONFIG_NAME
        ds.creator=ROOT_CONFIG_NAME
        ds.revision=1
                   
        self.session.merge(ds)
        self.session.commit()          
             
    
    def _drop(self, s):
        
        if not self.enable_delete:
            raise Exception("Deleting not enabled")
        
        for table in reversed(self.metadata.sorted_tables): # sorted by foreign key dependency
            table.drop(self.engine, checkfirst=True)

    def drop(self):
        s = self.session

        self._drop(s)
        s.commit()


    def create_tables(self):
        from databundles.orm import  Dataset, Partition, Table, Column, File, Config

        if self.driver == 'sqlite':

            dir_ = os.path.dirname(self.dbname)
            if not os.path.exists(dir_):
                try:
                    os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
                except Exception as e: #@UnusedVariable
                    pass
                
                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory "+dir_)
 
        tables = [ Dataset, Config, Table, Column, File, Partition]

        self.drop()

        for table in tables:
            table.__table__.create(bind=self.engine)

        self.session.commit()

    def install_bundle_file(self, identity, bundle_file):
        """Install a bundle in the database, starting from a file that may
        be a partition or a bundle"""

        #
        # This is really just used to ignore partitions
        #

        if isinstance(identity , dict):
            identity = new_identity(identity)
            
        if identity.is_bundle:
            
            bundle = DbBundle(bundle_file)
            self.install_bundle(bundle)
        
        
    def install_bundle(self, bundle):
        '''Copy the schema and partitions lists into the library database
        
        '''
        from databundles.orm import Dataset, Config, Column, Table, Partition
        from databundles.bundle import Bundle
           
        if not isinstance(bundle, Bundle):
            raise ValueError("Can only install a  Bundle object")

            # The Tables only get installed when the dataset is installed, 
            # not for the partition
            
        self._mark_update()
                    
        # There should be only one dataset record in the 
        # bundle
        bdbs = bundle.database._unmanaged_session
    
        s = self.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)
 
        for config in bdbs.query(Config).all():
            s.merge(config)
            
        s.query(Partition).filter(Partition.d_vid == dataset.vid).delete()
            
        for table in dataset.tables:
            s.query(Column).filter(Column.t_vid == table.vid).delete()

        s.query(Table).filter(Table.d_vid == dataset.vid).delete()
            
        for table in dataset.tables:
            
            s.merge(table)
         
            for column in table.columns:
                s.merge(column)

        for partition in dataset.partitions:
            s.merge(partition)
            
        try:

            s.commit()
        except IntegrityError as e:
            self.logger.error("Failed to merge")
            s.rollback()
            raise e

        
    def remove_bundle(self, bundle):
        '''remove a bundle from the database'''
        
        from databundles.orm import Dataset
        
        s = self.session

        dataset, partition = self.get_id(bundle.identity.vid) #@UnusedVariable

        if not dataset:
            return False

        dataset = s.query(Dataset).filter(Dataset.vid==dataset.vid).one()

        # Can't use delete() on the query -- bulk delete queries do not 
        # trigger in-python cascades!
        s.delete(dataset)
  
        s.commit()
        
      
    def get_id(self, id_):
        
        queries = [QueryCommand().identity(vid=id_),                   
                   QueryCommand().partition(vid=id_),
                   QueryCommand().identity(id=id_),
                   QueryCommand().partition(id=id_)]

        for q in queries:
            
            r = self.find(q)

            if len(r) > 0:
                r  = r[0]
                return new_identity(r['identity']),new_identity(r['partition']) if 'partition' in r else None
                       
            

        return False, False

    def get_name(self, name):
        
        queries = [QueryCommand().identity(vname=name),                   
                   QueryCommand().partition(vname=name),
                   QueryCommand().identity(name=name),
                   QueryCommand().partition(name=name)]

        for q in queries:
            
            r = self.find(q)

            if len(r) > 0:
                r  = r[0]
                return new_identity(r['identity']),new_identity(r['partition']) if 'partition' in r else None

        return False, False


        
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
        from databundles.orm import Table, Column
        
        def like_or_eq(c,v):
            
            if '%' in v:
                return c.like(v)
            else:
                return c == v
            
        
        s = self.session

        has_partition = False
        has_where = False
        
        if isinstance(query_command, Identity):
            raise NotImplementedError()
            out = []
            for d in self.queryByIdentity(query_command).all():
                id_ = d.identity
                d.path = os.path.join(self.cache,id_.cache_key)
                out.append(d)

        tables = [Dataset]
        
        if len(query_command.partition) > 0:
            tables.append(Partition)
            
        if len(query_command.table) > 0:
            tables.append(Table)      
                  
        if len(query_command.column) > 0:
            tables.append(Column)               

        tables.append(Dataset.id_) # Dataset.id_ is included to ensure result is always a tuple)

        query = s.query(*tables) # Dataset.id_ is included to ensure result is always a tuple
    
        if len(query_command.identity) > 0:
            for k,v in query_command.identity.items():
                if k == 'id':
                    k = 'id_'
                try:
                    query = query.filter( like_or_eq(getattr(Dataset, k),v) ) 
                except AttributeError as e:
                    # Dataset doesn't have the attribute, so ignore it. 
                    pass

        if len(query_command.partition) > 0:     
            query = query.join(Partition)
            has_partition = True
            for k,v in query_command.partition.items():
                if k == 'id':
                    k = 'id_'
                
                from sqlalchemy.sql import or_
                
                if k == 'any': 
                    continue # Just join the partition
                elif k == 'table':
                    # The 'table" value could be the table id
                    # or a table name
                    query = query.join(Table)
                    query = query.filter( or_(Partition.t_id  == v,
                                              like_or_eq(Table.name,v)))
                elif k == 'space':
                    query = query.filter( or_( like_or_eq(Partition.space,v)))
                    
                else:
                    query = query.filter(  like_or_eq(getattr(Partition, k),v) )
        
        if len(query_command.table) > 0:
            query = query.join(Table)
            for k,v in query_command.table.items():
                query = query.filter(  like_or_eq(getattr(Table, k),v) )

        if len(query_command.column) > 0:
            query = query.join(Table)
            query = query.join(Column)
            for k,v in query_command.column.items():
                query = query.filter(  like_or_eq(getattr(Column, k),v) )

        query = query.distinct().order_by(Dataset.revision.desc())

        out = []
        
        try:
            for r in query.all():
               
                o = {}
    
                try: 
                    o['identity'] = r.Dataset.identity.to_dict()
                    o['partition'] = r.Partition.identity.to_dict()
                   
                except: 
                    o['identity'] =  r.Dataset.identity.to_dict()
    
    
                try: o['table'] = r.Table.to_dict()
                except: pass
                
                try:o['column'] = r.Column.to_dict()
                except: pass
                
                out.append(o)
        except:

            raise

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

    def add_remote_file(self, identity):
        self.add_file(identity.cache_key, 'remote', identity.vid, state='remote')

        
    def add_file(self,path, group, ref, state='new', type_='bundle', data=None, source_url=None):
        from databundles.orm import  File

        if os.path.exists(path):
            stat = os.stat(path)
            modified = int(stat.st_mtime)
            size = stat.st_size
        else:
            modified = None
            size = None
      
        s = self.session

        try: s.query(File).filter(File.path == path).delete()
        except ProgrammingError: 
            pass
        except OperationalError: 
            pass
      
        file_ = File(path=path, 
                     group=group, 
                     ref=ref,
                     modified=modified, 
                     state = state,
                     size=size,
                     type_=type_,
                     data=data,
                     source_url=source_url
                     )
    
        # Sqlalchemy doesn't automatically rollback on exceptsions, and you
        # can't re-try the commit until you roll back. 
        try:
            s.add(file_)
            s.commit()
        except:
            s.rollback()
            raise
        
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
                     modified=int(time.time()),
                     state = state,
                     size=0)
    
        s.add(file_)
        s.commit()       
        
    def get_ticket(self, ticket):
        from databundles.orm import  File
        s = self.session
        
        return s.query(File).filter(File.group == ticket).one()
        

    def get_file_by_state(self, state, type_=None):
        """Return all files in the database with the given state"""
        from databundles.orm import  File
        s = self.session
        
        # The orderby clause should put bundles before partitions, which is 
        # required to install correctly. 
        
        if state == 'all':
            q =  s.query(File).order_by(File.ref)
        else:
            q =  s.query(File).filter(File.state == state).order_by(File.ref)
            
        if type_:
            q = q.filter(File.type_ == type_)
            
        return q.all()

    def get_file_by_ref(self, ref, type_=None):
        """Return all files in the database with the given state"""
        from databundles.orm import  File
        from sqlalchemy.orm.exc import NoResultFound 
        s = self.session

        try:
            q = s.query(File).filter(File.ref == ref)

            if type:
                q = q.filter(File.type_ == type_)
                
            return q.all()
        
        except NoResultFound:
            return None

    def get_file_by_type(self, type_=None):
        """Return all files in the database with the given state"""
        from databundles.orm import  File
        from sqlalchemy.orm.exc import NoResultFound 
        s = self.session

        try:
            return s.query(File).filter(File.type_ == type_).all()

        except NoResultFound:
            return None

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
        from databundles.orm import Dataset
        from sqlalchemy.orm.exc import NoResultFound 
        
        try: 
            dst.session.query(Dataset).filter(Dataset.vid=='a0').delete()
        except:
            pass
            
        for table in self.metadata.sorted_tables: # sorted by foreign key dependency

            rows = src.session.execute(table.select()).fetchall()
            dst.session.execute(table.delete())
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
        name
        vname
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
        id
        name
        vname
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

    class ParseError(Exception):
        pass
        
    @classmethod
    def parse(cls, s):

        from io import StringIO
        import tokenize, token
        
        state = 'name_start'
        n1 = None
        n2 = None
        value = None
        is_like = False
        
        qc = QueryCommand()

        for tt in tokenize.generate_tokens(StringIO(unicode(s)).readline):
            t_type =  tt[0]
            t_string = tt[1].strip()
            pos = tt[2][0]

            line = tt[4]
            
            #print "{:5d} {:5d} {:15s} || {}".format(t_type, pos, "'"+t_string+"'", line)
        

            def err(expected):
                raise cls.ParseError("Expected {} in {} at char {}, got {}, '{}' ".format(expected, line, pos, token.tok_name[t_type], t_string))

            if not t_string:
                continue

            if state == 'name_start':
                # First part of name
                if t_type == token.NAME:
                    n1 = t_string
                    state = 'name_sep'
                elif t_type == token.OP and t_string == ',':
                    state = 'name_start'
                elif t_type == token.ENDMARKER:
                    state = 'done'
                else:
                    err( "NAME or ',' ")
            elif state == 'name_sep':
                # '.' that serpates names
                if t_type == token.OP and t_string == '.':
                    state = 'name_2'
                else:
                    raise err("'.'")
            elif state == 'name_2':
                # Second part of name
                if t_type == token.NAME:
                    state = 'value_sep'
                    n2 = t_string
                else:
                    raise err("NAME")  
            elif state == 'value_sep':
                # The '=' that seperates name from values
                if (t_type == token.OP and t_string == '=') or (t_type == token.NAME and t_string == 'like'):
                    state = 'value'
                    
                    if t_string == 'like':
                        is_like = True
                    
                else:
                    raise err("'='")                            
            elif state == 'value':
                # The Value
                if t_type == token.NAME or t_type == token.STRING or t_type == token.NUMBER:
                    value = t_string
                    if is_like:
                        value = '%'+value+'%'
                        is_like = False
                    
                    state = 'name_start'
                   
                    qc.getsubdict(n1).__setattr__(n2,value.strip("'").strip('"'))
                   
                else:
                    raise err("NAME or STRING")
            elif state == 'done':
                raise cls.ParseError("Got token after end")         
            else:
                raise cls.ParseError("Unknown state: {} at char {}".format(state))                                      
                    
        return qc
        

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
    
    def __init__(self, cache,database, name =None, remote=None, sync=False, require_upload = False, host=None,port = None):
        '''
        Libraries are constructed on the root cache name for the library. 
        If the cache does not exist, it will be created. 
        
        Args:
        
            cache: a path name to a directory where bundle files will be stored
            database: 
            remote: URL of a remote library, for fallback for get and put. 
            sync: If true, put to remote synchronously. Defaults to False. 
   
        '''
 
        self.name = name
        self.cache = cache
        self._database = database
        self._remote = remote
        self.sync = sync
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port
        self.dep_cb = None# Callback for dependency resolution
        self.require_upload = require_upload
        self._dependencies = None


        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = logging.getLogger(__name__)
        #self.logger.setLevel(logging.DEBUG)

        self.needs_update = False
    
    def __del__(self):
        pass # print  'closing Llibrary'
    
    def clone(self):
        
        return self.__class__(self.cache, self.database.clone(), self._remote, self.sync, self.require_upload, self.host, self.port)
    
    
    def list(self, with_meta = True):
        from orm import Dataset
        
        datasets = {}

        if self.remote:
            for k,v in self.remote.list(with_metadata=with_meta).items():
                if v and v['identity']['id'] != 'a0':
                    v['identity']['location'] = ['R',' ']
                    datasets[k] =  v['identity']


        for r in self.database.session.query(Dataset).filter(Dataset.id_ != 'a0').all():
            
            if r.identity.cache_key not in datasets:
                v = r.identity.to_dict()
                v['location'] = [' ','L']
                datasets[r.identity.cache_key] = v
            else:
                datasets[r.identity.cache_key]['location'][1] = 'L'
                

        return sorted(datasets.values(), key=lambda x: x['vname'])
    
    
    @property
    def remote(self):
        if self._remote:
            return self._remote # When it is a URL to a REST interface. 
        else:
            return None
    
    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return self._database
  
    def load(self, rel_path):
        '''Load a record into the cache from the remote'''
        from util import copy_file_or_flo
   
        if not self.remote.has(rel_path):
            return None
        
        source = self.remote.get_stream(rel_path)
        sink = self.cache.put_stream(rel_path)
   
        try:
            copy_file_or_flo(source, sink)
        except:
            self.cache.remove(rel_path, propagate=True)
            raise
        
        source.close()
        sink.close()
        
        return self.cache.path(rel_path)

    def get_ref(self,bp_id):
        from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber, Identity
        from identity import new_identity
        
        if not isinstance(bp_id, basestring):
            bp_id = bp_id.id_string()
        

        d, p = self.database.get_id(bp_id)
      
        if not d:
            d, p = self.database.get_name(bp_id)
      
        if not d:
            r = self.remote_find(bp_id)
            if r:
                r = r[0]
                d = new_identity(r['identity'])
                p = new_identity(r['partition']) if 'partition' in r else None
   
        
        return d, p

    def config(self, bp_id):
        
        from cache import RemoteMarker
        
        d,p = self.get_ref(bp_id)
        
        try:
            api = self.remote.get_upstream(RemoteMarker)
        except AttributeError: # No api
            api = self.remote
        
      
        if self.cache.has(d.cache_key):
            b = self.get(d.vid)
            config = b.db_config.dict
        
        elif api:
            from client.exceptions import NotFound
            
            try:
                r = api.get(d.vid, p.vid if p else None)
                if r:
                    remote_d = r['dataset']['config']
                    
            except NotFound as e:
                pass 
        else:
            return None

    def _get_remote_dataset(self, dataset, cb=None):
        from identity import new_identity
        from util import copy_file_or_flo

                
        try:# ORM Objects
            identity = new_identity(dataset.to_dict())
        except:# Tuples
            identity = new_identity(dataset._asdict())       

        source = self.remote.get_stream(identity.cache_key)
        
        if not source:
            return False
        
        # Store it in the local cache. 

        sink = self.cache.put_stream(identity.cache_key)
        
        try:
            copy_file_or_flo(source, sink, cb=cb)
        except:
            self.cache.remove(identity.cache_key, propagate=True)
            raise

        abs_path = self.cache.path(identity.cache_key)
        
        #if not self.cache.has(identity.cache_key):
        #    abs_path = self.cache.put(r, identity.cache_key )
        #else:
        #    abs_path = self.cache.get(identity.cache_key )
        
        if not os.path.exists(abs_path):
            raise Exception("Didn't get file '{}' for id {}".format(abs_path, identity.cache_key))
      
        return abs_path
      
    def _get_remote_partition(self, bundle, partition, cb = None):

        from identity import  new_identity 
        from util import copy_file_or_flo

        identity = new_identity(partition.to_dict(), bundle=bundle) 

        p = bundle.partitions.get(identity.id_) # Get partition information from bundle
        
        if not p:
            from databundles.dbexceptions import NotFoundError
            raise NotFoundError("Failed to find partition {} in bundle {}"
                                .format(identity.name, bundle.identity.name))

        if os.path.exists(p.database.path):
            from databundles.dbexceptions import ConflictError
            raise ConflictError("Trying to get {}, but file {} already exists".format(identity.vname, p.database.path))

        # Now actually get it from the remote. 

        source, meta = self.remote.get_stream(p.identity.cache_key, return_meta=True)

        # Store it in the local cache. 
        sink = self.cache.put_stream(p.identity.cache_key)

        try:
            if cb:
                def progress_cb(i):
                    cb(i,meta['content-length'])
            else:
                progress_cb = None
                
            copy_file_or_flo(source, sink,cb=progress_cb)
            
        except:
            self.cache.remove(p.identity.cache_key, propagate = True)
            raise
            
        p_abs_path = self.cache.path(p.identity.cache_key)


        if os.path.realpath(p.database.path) != os.path.realpath(p_abs_path):
            m =( "Path mismatch in downloading partition: {} != {}"
                 .format(os.path.realpath(p.database.path),
                                os.path.realpath(p_abs_path)))
                   
            self.logger.error(m)
            raise Exception(m)

        return p_abs_path, p
            

    def get(self,bp_id, force = False, cb=None):
        '''Get a bundle, given an id string or a name '''

        # Get a reference to the dataset, partition and relative path
        # from the local database. 

        dataset, partition = self.get_ref(bp_id)

        if partition:
            return self._get_partition(dataset, partition, force, cb=cb)
        elif dataset:
            return self._get_dataset(dataset, force, cb=cb)
        else:
            return False

    def _get_dataset(self, dataset, force = False, cb=None):

        # Try to get the file from the cache. 
        abs_path = self.cache.get(dataset.cache_key, cb=cb)

        # Not in the cache, try to get it from the remote library, 
        # if a remote was set. 

        if ( not abs_path or force )  and self.remote :
            abs_path = self._get_remote_dataset(dataset)
            
        if not abs_path or not os.path.exists(abs_path):
            return False

        bundle = DbBundle(abs_path)

        # Do we have it in the database? If not install it. 
        # It should be installed if it was retrieved remotely, 
        # but may not be installed if there is a local copy in the dcache. 
        d, p = self.database.get_id(bundle.identity.vid)
        if not d:
            self.database.install_bundle(bundle)
            self.database.add_file(abs_path, self.cache.repo_id, bundle.identity.vid, 'pulled')   

        bundle.library = self

        return bundle
    
    def _get_partition(self,  dataset, partition, force = False, cb=None):
        from databundles.dbexceptions import NotFoundError
        from databundles.client.exceptions import NotFound as RemoteNotFound

        r = self._get_dataset(dataset, cb=cb)

        if not r:
            return False

        try:
            p =  r.partitions.partition(partition)
        except:
            raise NotFoundError("Partition '{}' not in bundle  '{}' ".format(partition, r.identity.name ))
        
        rp = self.cache.get(p.identity.cache_key, cb=cb)

        if not os.path.exists(p.database.path) or p.database.is_empty() or force:

            if self.remote:
                try:
                    self._get_remote_partition(r,partition, cb=cb)
                except RemoteNotFound:
                    raise NotFoundError("""Didn't find partition {} in bundle {}. Partition found in bundle, but path {} ({}?) not in local library and doesn't have it either. """
                                   .format(p.identity.name,r.identity.name,p.database.path, rp))
             
            else:
                raise NotFoundError("""Didn't find partition {} in bundle {}. Partition found in bundle, but path {} ({}?) not in local library and remote not set. """
                               .format(p.identity.name, r.identity.name,p.database.path, rp))
        
        
            # Ensure the file is in the local library. 

        ds, pt= self.database.get_id(p.identity.vid)
        if not pt:
            self.database.add_file(p.database.path, self.cache.repo_id, p.identity.vid, 'pulled')   
        
        p.library = self  
        
        # Unsetting the database is particularly necessary for Hdf partitions where the file will get loaded when the partition
        # is first created, which creates and empty file, and the empty file is carried forward after the file on 
        # disk is changed.  
        p.unset_database()
        r.partition = p

        return r

        
    def find(self, query_command):

        return self.database.find(query_command)
        
        
    def remote_find(self, query_command):
        from cache import RemoteMarker
        import socket

        try:
            api = self.remote.get_upstream(RemoteMarker).api
        except AttributeError: # No api
            try:
                api = self.remote.api
            except AttributeError: # No api 
                return False
            
        try:
            r = api.find(query_command)
        except socket.error:
            self.logger.error("Connection to remote failed")
            return False
        
        if not r:
            return False

        return r

    def path(self, rel_path):
        """Return the cache path for a cache key"""
        
        return self.cache.path(rel_path)
        
        
    @property
    def dependencies(self):
        
        if not self._dependencies:
            self ._dependencies = self._get_dependencies()
            
        return self._dependencies
        
    def clear_dependencies(self):
        self._dependencies = None

    def _get_dependencies(self):
        from databundles.identity import Identity
        
        if not self.bundle:
            raise ConfigurationError("Can't use the dep() method for a library that is not attached to a bundle");


        group = self.bundle.config.group('build')
        
        try:
            deps = group.get('dependencies')
        except AttributeError:
            deps = None
            
        if not deps:
            return {}
        
        out = {}
        for k,v in deps.items():
            
            try: 
                Identity.parse_name(v)
                out[k] = v
            except Exception as e:
                self.bundle.error("Failed to parse dependency name '{}' for '{}': {}".format(v, self.bundle.identity.name, e.message))

        return out
             
    def check_dependencies(self, throw=True):

        errors = {}
        for k,v in self.dependencies.items():
            b = self.get(v)
             
            if not b:
                if throw:
                    raise NotFoundError("Dependency check failed for key={}, id={}".format(k, v))
                else:
                    errors[k] = v


    def dep(self,name):
        """"Bundle version of get(), which uses a key in the 
        bundles configuration group 'dependencies' to resolve to a name"""

        bundle_name = self.dependencies.get(name, False)
        
        if not bundle_name:
            raise ConfigurationError("No dependency named '{}'".format(name))
        
        b = self.get(bundle_name)
        
        if not b:
            raise NotFoundError("Failed to get dependency, key={}, id={}".format(name, bundle_name))
        
        
        if self.dep_cb:
            self.dep_cb( self, name, bundle_name, b)
        
        return b

    def put_remote_ref(self,identity):
        '''Store a reference to a partition that has been uploaded directly to the remote'''
        pass
        
    def put_file(self, identity, file_path, state='new', force=False):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity''' 
        
        if isinstance(identity , dict):
            identity = new_identity(identity)

        if not self.cache.has(identity.cache_key) or force:
            dst = self.cache.put(file_path,identity.cache_key)
        else: 
            dst = self.cache.path(identity.cache_key)

        if not os.path.exists(dst):
            raise Exception("cache {}.put() didn't return an existent path. got: {}".format(type(self.cache), dst))

        if self.remote and self.sync:
            self.remote.put(identity, file_path)

        

        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ ='bundle')
        else:
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ = 'partition')

        return dst, identity.cache_key, self.cache.last_upstream().path(identity.cache_key)
     
    def put(self, bundle, force=False):
        '''Install a bundle or partition file into the library.
        
        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object
        
        '''
        from bundle import Bundle
        from partition import PartitionInterface
        
        if not isinstance(bundle, (PartitionInterface, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")
        
        
        bundle.identity.name # throw exception if not right type. 

        dst, cache_key, url = self.put_file(bundle.identity, bundle.database.path, force=force)

        return dst, cache_key, url

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.database.remove_bundle(bundle)
        
        self.cache.remove(bundle.identity.cache_key, propagate = True)
        
    def clean(self, add_config_root=True):
        self.database.clean(add_config_root=add_config_root)
        
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
            
            dataset, partition = self.database.get_id(file_.ref)
            
            if not dataset:
                raise Exception("Didn't get id from database for file ref: {}".format(file_.ref))
            
            if partition:
                identity = partition
            else:
                identity = dataset

            self.remote.put(file_.path, identity.cache_key, metadata=identity.to_meta(file=file_.path))
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

    def restore(self, backup_file=None):
        '''Restore the database from the remote'''

        if not backup_file:
            # This requires that the cache have and upstream that is also the remote
            backup_file = self.cache.get('_/library.db')

        self.database.restore(backup_file)

        # HACK, fix the dataset root
        try:
            self.database._clean_config_root()
        except:
            print "ERROR for path: {}, {}".format(self.database.dbname, self.database.dsn)
            raise 
        
        os.remove(backup_file)   

        return backup_file        

    def remote_rebuild(self):
        '''Rebuild the library from the contents of the remote'''

        self.logger.info("Rebuild library from: {}".format(self.remote))

        self.database.drop()
        self.database.create()
        
        for rel_path in self.remote.list():

            path = self.load(rel_path)
  
            if not path or not os.path.exists(path):
                self.logger.error("ERROR: Failed to get load for relpath: '{}' ( '{}' )".format(rel_path, path))
                continue
     
            bundle = DbBundle(path)
            identity = bundle.identity
     
            self.database.add_file(path, self.cache.repo_id, identity.vid,  'pulled')
            self.logger.info('Installing: {} '.format(bundle.identity.name))
            self.database.install_bundle_file(identity, path)
            
            for p in bundle.partitions:  
                if self.remote.last_upstream().has(p.identity.cache_key):
                    self.database.add_remote_file(p.identity)
                    self.logger.info('            {} '.format(p.identity.name))



    def rebuild(self):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''
  
        from databundles.bundle import DbBundle
   
        self.database.drop()
        self.database.create()
   
        bundles = []
        for r,d,f in os.walk(self.cache.cache_dir): #@UnusedVariable
            for file_ in f:
                
                if file_.endswith(".db"):
                    path_ = os.path.join(r,file_)
                    try:
                        b = DbBundle(path_)
                        # This is a fragile hack -- there should be a flag in the database
                        # that diferentiates a partition from a bundle. 
                        f = os.path.splitext(file_)[0]
    
                        if b.db_config.get_value('info','type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.name, file_))
                            bundles.append(b)
                            
                    except Exception as e:
                        pass
                        #self.logger.error('Failed to process {}, {} : {} '.format(file_, path_, e))

        
        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.name))
            self.database.install_bundle(bundle)
            self.database.add_file(bundle.database.path, self.cache.repo_id, bundle.identity.vid,  'rebuilt', type_='bundle')

            for p in bundle.partitions:
                
                if self.cache.has(p.identity.cache_key, use_upstream=False):
                    self.logger.info('            {} '.format(p.identity.name))
                    self.database.add_file(p.database.path, self.cache.repo_id, p.identity.vid,  'rebuilt', type_='partition')
    

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
