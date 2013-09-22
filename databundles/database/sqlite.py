
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from __future__ import absolute_import
from .relational import RelationalBundleDatabaseMixin, RelationalDatabase #@UnresolvedImport
import os

class SqliteDatabase(RelationalDatabase):

    EXTENSION = '.db'
    SCHEMA_VERSION = 11

    def __init__(self, dbname, memory = False,  **kwargs):   
        ''' '''
    
        # For database bundles, where we have to pass in the whole file path
        base_path, ext = os.path.splitext(dbname)
        
        if ext and ext != self.EXTENSION:
            raise Exception("Bad extension to file: {}: {}".format(base_path, ext))
        
        self.base_path = base_path

        self._last_attach_name = None
        self._attachments = set()

        # DB-API is needed to issue INSERT OR REPLACE type inserts. 
        self._dbapi_cursor = None
        self._dbapi_connection = None
        self.memory = memory

        kwargs['driver'] = 'sqlite'

        super(SqliteDatabase, self).__init__(dbname=self.path,   **kwargs)
        
    @property 
    def path(self):
        if self.memory:
            return ':memory:'
        else:
            return self.base_path+self.EXTENSION
     
    @property
    def lock_path(self):
        return self.base_path

    def require_path(self):
        if not self.memory:
            if not os.path.exists(os.path.dirname(self.base_path)):
                os.makedirs(os.path.dirname(self.base_path))
            
    @property
    def engine(self):
        return self._get_engine(_on_connect)
    
    def _get_engine(self, connect_listener):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        import sqlite3
        
        if not self._engine:
            self.require_path()
            self._engine = create_engine('sqlite:///'+self.path,
                                         connect_args={'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES},
                                         native_datetime=True,
                                         echo=False) 
            
            from sqlalchemy import event
            
            event.listen(self._engine, 'connect',connect_listener)
            #event.listen(self._engine, 'connect', _on_connect_update_schema)
            _on_connect_update_schema(self.connection)
             
        return self._engine

    @property
    def dbapi_connection(self):
        '''Return an DB_API connection'''
        import sqlite3
        if not self._dbapi_connection:
            self._dbapi_connection = sqlite3.connect(self.path)
            
        return self._dbapi_connection

    @property
    def dbapi_cursor(self):
        '''Return an DB_API cursor'''
        if not self._dbapi_cursor:
        
            self._dbapi_cursor = self.dbapi_connection.cursor()
            
        return self._dbapi_cursor
    
    def dbapi_close(self):
        '''Close both the cursor and the connection'''
        if  self._dbapi_cursor:
            self._dbapi_cursor.close();
            self._dbapi_cursor = None
            
        if  self._dbapi_connection:
            self._dbapi_connection.close();
            self._dbapi_connection = None    

    
    def exists(self):
        return os.path.exists( self.path)
    
    def is_empty(self):
        
        return not self.exists()
        
    def delete(self):
        
        try :
            os.remove(self.path)
        except:
            pass
        
    def clean(self):
        '''Remove all files generated by the build process'''
        os.remove(self.path)


    def load(self,a, table=None, encoding='utf-8', caster = None, logger=None):
        ''' Load the database from a CSV file '''
        
        #return self.load_insert(a,table, encoding=encoding, caster=caster, logger=logger)
        return self.load_shell(a,table, encoding=encoding, caster=caster, logger=logger)

    def load_insert(self,a, table=None, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time
        
        if isinstance(a,PartitionInterface):
            db = a.database
        elif isinstance(a,CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))

    
        start = time.clock()
        count = 0
        with self.inserter(table,  caster=caster) as ins:
            for row in db.reader(encoding=encoding):
                count+=1
             
                if logger:
                    logger("Load row {}:".format(count))
             
                ins.insert(row)
        
        diff = time.clock() - start
        return count, diff
        
    def load_shell(self,a, table, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time
        import subprocess, uuid
        from ..util import temp_file_name
        import os
        
        if isinstance(a,PartitionInterface):
            db = a.database
        elif isinstance(a,CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))
        

        try: table_name = table.name
        except AttributeError: table_name = table
        
        sql_file = temp_file_name()
        
        sql = '''
.mode csv
.separator '|'
select 'Loading CSV file','{path}';
.import {path} {table}
'''.format(path=db.path, table=table_name)

        sqlite = subprocess.check_output(["which", "sqlite3"]).strip()

        start = time.clock()
        count = 0

        command = "{sqlite} {database}  < {sql_file} ".format(sqlite=sqlite, database=self.path, sql_file=sql_file)

        proc = subprocess.Popen([sqlite,  self.path], stdout=subprocess.PIPE,  stderr=subprocess.PIPE, stdin=subprocess.PIPE)

        (out, err) = proc.communicate(input=sql)
        
        if proc.returncode != 0:
            raise Exception("Database load failed: "+str(err))

        diff = time.clock() - start
        return count, diff


class BundleLockContext(object):
    
    def __init__( self, bundle):

        from lockfile import FileLock

        self._bundle = bundle
        self._lock_path = self._bundle.path

        self._lock = FileLock(self._lock_path)

            
    def __enter__( self ):
        from sqlalchemy.orm import sessionmaker
        
        if self._bundle._session:
            raise Exception("Bundle already has a session")
        
        Session = sessionmaker(bind=self._bundle.engine,autocommit=False)
        self._session =  Session()

        #print " #### LOCKING ", self._lock_path
        self._lock.acquire()

        self._bundle._session = self._session
        return self._session
    
    def __exit__( self, exc_type, exc_val, exc_tb ):

        if  exc_type is not None:
            self._session.rollback()
            self._lock.release()
            self._bundle._session.close()
            self._bundle._session = None
            #print " #### UNLOCKED w/Exception", self._lock_path
            return False
        else:
            #print " #### UNLOCKING ", self._lock_path
            self._session.commit()
            self._lock.release()
            self._bundle._session.close()
            self._bundle._session = None
            #print " #### UNLOCKED ", self._lock_path
            return True
            

class SqliteBundleDatabase(RelationalBundleDatabaseMixin,SqliteDatabase):


    def __init__(self, bundle, dbname, **kwargs):   
        '''
        '''

        RelationalBundleDatabaseMixin._init(self, bundle)
        super(SqliteBundleDatabase, self).__init__(dbname,  **kwargs)

        self._session = None # This is controlled by the BundleLockContext

    def create(self):

        self.require_path()
  
        if RelationalDatabase._create(self):
            
            RelationalBundleDatabaseMixin._create(self)

            self._unmanaged_session.execute("PRAGMA user_version = {}".format(self.SCHEMA_VERSION))

            self.post_create()
            
            self._unmanaged_commit()

        
    @property
    def engine(self):
        return self._get_engine(_on_connect_bundle)
        
    @property
    def session(self):
        from ..dbexceptions import  NoLock
        
        if not self._session:
            raise NoLock("Must use bundle.lock to acquire a session lock")
        
        return self._session
        
    @property
    def has_session(self):
        return self._session is not None
                
    @property
    def lock(self):
        return BundleLockContext(self)
        
            
    def copy_table_from(self, source_db, table_name):
        '''Copy the definition of a table from a soruce database to this one
        
        Args:
            table. The name or Id of the table
        
        '''
        from databundles.schema import Schema
        
        table = Schema.get_table_from_database(source_db, table_name)

        with self.session_context as s:
            table.session_id = None
         
            s.merge(table)
            s.commit()
            
            for column in table.columns:
                column.session_id = None
                s.merge(column)

        return table
    


class SqliteWarehouseDatabase(SqliteDatabase):

    def __init__(self, dbname, **kwargs):   
        '''
        '''

        super(SqliteWarehouseDatabase, self).__init__(dbname,  **kwargs)


def _on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')
    dbapi_con.execute('PRAGMA journal_mode = OFF')
    #dbapi_con.execute('PRAGMA synchronous = OFF')
    #dbapi_con.enable_load_extension(True)

def _on_connect_bundle(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created
    
    Bundles have different parameters because they are more likely to be accessed concurrently. 
    '''
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')




def _on_connect_update_schema(conn):
    '''Perform on-the-fly schema updates based on the user version'''

    version = conn.execute('PRAGMA user_version').fetchone()[0]

    if version < 10:
        
        try: conn.execute('ALTER TABLE columns ADD COLUMN c_foreign_key VARCHAR(50);')
        except: pass
        
        try: conn.execute('ALTER TABLE partitions ADD COLUMN p_format VARCHAR(50);')
        except: pass
        
        try: conn.execute('ALTER TABLE partitions ADD COLUMN p_segment INTEGER;')
        except: pass
                
        conn.execute('PRAGMA user_version = 10;')

    if version < 11:
        
        try: conn.execute('ALTER TABLE partitions ADD COLUMN p_min_key INTEGER;')
        except: pass
        
        try: conn.execute('ALTER TABLE partitions ADD COLUMN p_max_key INTEGER;')
        except: pass
        
        try: conn.execute('ALTER TABLE partitions ADD COLUMN p_count INTEGER;')
        except: pass
                
        conn.execute('PRAGMA user_version = 11')

class BuildBundleDb(SqliteBundleDatabase):
    '''For Bundle databases when they are being built, and the path is computed from 
    the build base director'''
    @property 
    def path(self):
        return self.bundle.path + self.EXTENSION
    
 
def insert_or_ignore(table, columns):
    return  ("""INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})"""
                            .format(
                                 table=table,
                                 columns =','.join([c.name for c in columns ]),
                                 values = ','.join(['?' for c in columns]) #@UnusedVariable
                            )
                         )
    


  
