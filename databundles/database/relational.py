
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import DatabaseInterface #@UnresolvedImport
from .inserter import  ValueInserter, ValueUpdater 
import os 
import logging
from databundles.util import get_logger

class RelationalDatabase(DatabaseInterface):
    '''Represents a Sqlite database'''

    DBCI = {
            'postgres':'postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}', # Stored in the databundles module. 
            'sqlite':'sqlite:///{name}',
            'mysql':'mysql://{user}:{password}@{server}{colon_port}/{name}'
            }
    
    dsn = None

    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None, port=None,  **kwargs):

        '''Initialize the a database object
        
        Args:
            bundle. a Bundle object
            
            base_path. Path to the database file. If None, uses the name of the
            bundle, in the bundle build director. 
            
            post_create. A function called during the create() method. has
            signature post_create(database)
       
        '''
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
   
        if port:
            self.colon_port = ':'+str(port)
        else:
            self.colon_port = ''
                
        self._engine = None
        self._session = None
        self._connection = None

    
        self._table_meta_cache = {}

        self.dsn_template = self.DBCI[self.driver]
        self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                    server=self.server, name=self.dbname, colon_port=self.colon_port)
       
        self.logger = get_logger(__name__)
        self.logger.setLevel(logging.INFO) 

    def log(self,message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)

    def create(self):
        self.connection
        
        return True
    
    def exists(self):
        self.connection
        
        return True

    def _create(self):
        """Create the database from the base SQL"""
        from databundles.orm import  Config
        if not self.exists():    

            self.require_path()
                     
            s =  self.session
         
            tables = [ Config ]

            for table in tables:
                table.__table__.create(bind=self.engine)
    
            self.session.commit()

            return True #signal did create
            
        return False # signal didn't create

    def _post_create(self):
        # call the post create function
        from ..orm import Config
        from datetime import datetime
        
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'process','dbcreated', datetime.now().isoformat() )
        
    def post_create(self):
        '''Call all implementations of _post_create in this object's class heirarchy'''
        import inspect

        for cls in inspect.getmro(self.__class__):
            for n,f in inspect.getmembers(cls,lambda m: inspect.ismethod(m) and m.__func__ in m.im_class.__dict__.values()):
                if n == '_post_create':
                    f(self)

    def _drop(self, s):
        
        if not self.enable_delete:
            raise Exception("Deleting not enabled")
        
        for table in reversed(self.metadata.sorted_tables): # sorted by foreign key dependency
            table.drop(self.engine, checkfirst=True)

    def drop(self):
        s = self.session

        self._drop(s)
        s.commit()

    @property
    def connection(self):
        '''Return an SqlAlchemy connection'''
        if not self._connection:
            try:
                self._connection = self.engine.connect()
            except Exception as e:
                self.error("Failed to open: '{}' ".format(self.path))
                raise
            
        return self._connection
    
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  

        if not self._engine:
            self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                            server=self.server, name=self.dbname, colon_port=self.colon_port)

            self._engine = create_engine(self.dsn, echo=False) 

        return self._engine


    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''
        
        from sqlalchemy import MetaData   
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
    
        return meta
    
    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)

    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        
        if not self._session:    
            Session = sessionmaker(bind=self.engine,autocommit=False)
            self._session = Session()
            
        return self._session
   
    def open(self):
        # Fetching the connection objects the database
        # This isn't necessary for Sqlite databases. 
        return self.connection
   
    def close(self):
        if self._session:    
            self._session.close()
            self._session = None
        
        if self._connection:
            self._connection.close()
            self._connection = None
   
    def commit(self):
        self.session.commit()

    def clean_table(self, table):

        if isinstance(table, basestring):
            self.connection.execute('DELETE FROM {} '.format(table))
        else:
            self.connection.execute('DELETE FROM {} '.format(table.name))
            
        self.commit()

    def create_table(self, table_name):
        '''Create a table that is defined in the table table
        
        This method will issue the DDL to create a table that is defined
        in the meta data tables, of which the 'table' table ho;d information
        about tables.
        
        Args:
            table_name. The name of the table to create
        
        '''
        
        if not table_name in self.inspector.get_table_names():
            t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
            t_meta.create(bind=self.engine)
            
            if not table_name in self.inspector.get_table_names():
                raise Exception("Don't have table "+table_name)
             
    def tables(self):
        
        return self.metadata.sorted_tables
                   
    def table(self, table_name): 
        '''Get table metadata from the database''' 
        from sqlalchemy import Table
        
        table = self._table_meta_cache.get(table_name, False)
        
        if table is not False:
            return table
        else:
            metadata = self.metadata
            table = Table(table_name, metadata, autoload=True)
            self._table_meta_cache[table_name] = table
            return table

    def inserter(self,table_name, **kwargs):
        from sqlalchemy.schema import Table

        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)

        
        return ValueInserter(None, table , self,**kwargs)


    def set_config_value(self, d_vid, group, key, value):
        from databundles.orm import Config as SAConfig
        
        if group == 'identity' and d_vid != SAConfig.ROOT_CONFIG_NAME_V:
            raise ValueError("Can't set identity group from this interface. Use the dataset")
        
        s = self.session
     
        key = key.strip('_')
  
        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == d_vid).delete()
        

        o = SAConfig(group=group, key=key,d_vid=d_vid,value = value)
        s.add(o)
        s.commit() 


    def get_config_value(self, d_vid, group, key):
        from databundles.orm import Config as SAConfig
        
        s = self.session
     
        key = key.strip('_')
  
        return s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == d_vid).first()
        
        

class RelationalBundleDatabaseMixin(object):
    
    bundle = None
    
    def _init(self, bundle, **kwargs):   

        self.bundle = bundle 

    def _create(self):
        """Create the database from the base SQL"""
        from databundles.orm import  Dataset, Partition, Table, Column, File

                 
        s =  self.session
     
        tables = [ Dataset, Partition, Table, Column, File ]

        for table in tables:
            table.__table__.create(bind=self.engine)

        self.session.commit()
        
        # Create the Dataset record

        ds = Dataset(**self.bundle.config.identity)
        ds.name = self.bundle.config.identity.name
        ds.vname = self.bundle.config.identity.vname
        
        s.add(ds)
        s.commit()

    def rewrite_dataset(self):
        from ..orm import Dataset
        # Now patch up the Dataset object
        
        ds = Dataset(**self.bundle.identity.to_dict())
        ds.name = self.bundle.identity.name
        ds.vname = self.bundle.identity.vname
        
        s = self.session
        s.merge(ds)
        s.commit()
        

    def _post_create(self):
        from ..orm import Config
        self.set_config_value(self.bundle.identity.vid, 'info','type', 'bundle' )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vname', self.bundle.identity.vname )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vid', self.bundle.identity.vid )


class RelationalPartitionDatabaseMixin(object):
    
    bundle = None
    
    def _init(self, bundle, partition, **kwargs):   

        self.partition = partition
        self.bundle = bundle 

    def _post_create(self):
        from ..orm import Config

        self.set_config_value(self.bundle.identity.vid, 'info','type', 'partition' )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vname', self.bundle.identity.vname )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vid', self.bundle.identity.vid )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'partition','vname', self.partition.identity.vname )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'partition','vid', self.partition.identity.vid )






