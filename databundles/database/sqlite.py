
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
     

    def require_path(self):
        if not self.memory:
            if not os.path.exists(os.path.dirname(self.base_path)):
                os.makedirs(os.path.dirname(self.base_path))
            
    @property
    def engine(self):
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
            
            event.listen(self._engine, 'connect', _on_connect)
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
        return self.load_insert(a,table, encoding=encoding, caster=caster, logger=logger)

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
        
    def load_shell(self,a, table=None, encoding='utf-8', caster=None, logger=None):
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
        
        sql_file = temp_file_name()
        
        sql = '''
.mode csv
.separator '|'
select 'Loading CSV file','{path}';
.import {path} {table}
'''.format(path=db.path, table=table.name)
        
        sqlite = subprocess.check_output(["which", "sqlite3"]).strip()
        
        print sqlite
        
        start = time.clock()
        count = 0
        
        with open(sql_file,'wb') as f:
            f.write(sql)
        
        command = "{sqlite} {database}  < {sql_file} ".format(sqlite=sqlite, database=self.path, sql_file=sql_file)
        os.system(command)
        
        diff = time.clock() - start
        return count, diff


class SqliteBundleDatabase(RelationalBundleDatabaseMixin,SqliteDatabase):


    def __init__(self, bundle, dbname, **kwargs):   
        '''
        '''

        RelationalBundleDatabaseMixin._init(self, bundle)
        super(SqliteBundleDatabase, self).__init__(dbname,  **kwargs)

    def create(self):

        self.require_path()
        
        if RelationalDatabase._create(self):
            
            RelationalBundleDatabaseMixin._create(self)
            
            s =  self.session
            s.execute("PRAGMA user_version = {}".format(self.SCHEMA_VERSION))
            s.commit()
            
            self.post_create()
        
            
    def copy_table_from(self, source_db, table_name):
        '''Copy the definition of a table from a soruce database to this one
        
        Args:
            table. The name or Id of the table
        
        '''
        from databundles.schema import Schema
        
        table = Schema.get_table_from_database(source_db, table_name)
        
        s = self.session
        
        table.session_id = None
     
        s.merge(table)
        s.commit()
        
        for column in table.columns:
            column.session_id = None
            s.merge(column)
        
        s.commit()
        
        return table
    
    def commit(self):
        print "UNLOCKED COMMIT"
        super(SqliteBundleDatabase, self).commit()
     
    
    def locked_commit(self):
        '''Acquire a file lock before committing the session. we will wait for this lock
        much longer than the lock internal to Sqlite '''

        with self.lock:
            return self.session.commit()
    
    @property
    def lock(self):
        '''Return a file lock on the database'''
        from lockfile import FileLock
        path =   self.base_path+'.lock'
        return FileLock(path)


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
    


  
