"""Base class for Bundle and Partition databases. This module also includes
interfaces for temporary CSV files and HDF files.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
import anydbm
from databundles.geo.sfschema import TableShapefile
from .hdf5 import Hdf5File

class FeatureInserter(object):
    def __init__(self, partition, table, dest_srs=4326, source_srs=None):

        self.bundle = partition.bundle
        
        self.sf = TableShapefile(self.bundle, partition.database.path, table, dest_srs, source_srs)
        
    
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self
    
    def insert(self, row, source_srs=None):
        from sqlalchemy.engine.result import RowProxy
        
        if isinstance(row, RowProxy):
            row  = dict(row)
        
        return self.sf.add_feature( row, source_srs)
    
    def close(self):
        self.sf.close()

class ValueWriter(object):
    '''Inserts arrays of values into  database table'''
    def __init__(self, bundle,  db, cache_size=50000, text_factory = None, replace=False):
        import string 
        self.bundle = bundle
        self.db = db
        self.session = self.db.session
        self.connection = self.db.connection
        self.cache = []
        self.transaction = None
        self.cache_size = cache_size
        self.statement = None
        
        if text_factory:
            self.db.engine.raw_connection().connection.text_factory = text_factory

    def __enter__(self): 
        self.transaction = self.connection.begin()
        return self
        
    def close(self):

        if len(self.cache) > 0 :       
            try:
                self.connection.execute(self.statement, self.cache)
                if self.transaction:
                    self.transaction.commit()
                    self.transaction = None
                self.cache = []
            except (KeyboardInterrupt, SystemExit):
                if self.transaction:
                    self.transaction.rollback()
                    self.transaction = None
                raise
            except Exception as e:
                self.bundle.error("Exception during ValueInserter.insert: "+str(e))
                if self.transaction:
                    self.transaction.rollback()
                    self.transaction = None
                raise
        else:
            if self.transaction:
                self.transaction.commit()    
                    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self
        
 
class ValueInserter(ValueWriter):
    '''Inserts arrays of values into  database table'''
    def __init__(self, bundle, table, db, cache_size=50000, text_factory = None, replace=False): 
        super(ValueInserter, self).__init__(bundle, db, cache_size=cache_size, text_factory = text_factory)  
   
        self.table = table
        
        self.header = [c.name for c in self.table.columns]
   
        self.statement = self.table.insert()
        if replace:
            self.statement = self.statement.prefix_with('OR REPLACE')


    def insert(self, values):
      
        try:
            if isinstance(values, dict):
                d = values
            else:
                d  = dict(zip(self.header, values))
         
            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size:
                
                self.connection.execute(self.statement, self.cache)
                self.cache = []
            
                
        except (KeyboardInterrupt, SystemExit):
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise
        except Exception as e:
            self.bundle.error("Exception during ValueInserter.insert: "+str(e))
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise e

        return True
   
class ValueUpdater(ValueWriter):
    '''Updates arrays of values into  database table'''
    def __init__(self, bundle, table, db,  cache_size=50000, text_factory = None): 
        
        from sqlalchemy.sql.expression import bindparam, and_
        super(ValueUpdater, self).__init__(bundle, db, cache_size=50000, text_factory = text_factory)  
    
        self.table = table
        self.statement = self.table.update()
     
        wheres = []
        for primary_key in table.primary_key:
            wheres.append(primary_key == bindparam('_'+primary_key.name))
            
        if len(wheres) == 1:
            self.statement = self.statement.where(wheres[0])
        else:
            self.statement = self.statement.where(and_(wheres))
       
        self.values = None
       

    def update(self, values):
        from sqlalchemy.sql.expression import bindparam
        
        if not self.values:
            names = values.keys()
            
            binds = {}
            for col_name in names:
                if not col_name.startswith("_"):
                    raise ValueError("Columns names must start with _ for use in updater")
                
                column = self.table.c[col_name[1:]]
                binds[column.name] = bindparam(col_name)
                
                self.statement = self.statement.values(**binds)
       
        try:
            if isinstance(values, dict):
                d = values
            else:
                d  = dict(zip(self.header, values))
         
            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size:
                
                self.connection.execute(self.statement, self.cache)
                self.cache = []
                
        except (KeyboardInterrupt, SystemExit):
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise
        except Exception as e:
            self.bundle.error("Exception during ValueInserter.insert: "+str(e))
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise e

        return True    
class TempFile(object): 
           
    def __init__(self, bundle,  db, table, suffix=None, header=None, ignore_first=False):
        self.bundle = bundle
        self.db = db 
        self.table = table
        self.file = None
        self.suffix = suffix
        self.ignore_first = ignore_first

        if header is None:
            header = [ c.name for c in table.columns ]
        else:
            pass

        self.header = header

        name = table.name
        
        if suffix:
            name += "-"+suffix

        self._path = str(self.db.path)+'.d/'+name+".csv"
        
        self._writer = None
        self._reader = None
        
    def __enter__(self): 
        return self
        
    @property
    def writer(self):
        if self._writer is None:
            import csv
            self.close()
            
            if self.exists:
                mode = 'a+'

            else:
                mode = 'w'
                try: os.makedirs(os.path.dirname(self.path))
                except: pass

            self.file = open(self.path, mode)
            self._writer = csv.writer(self.file)
            
            if mode == 'w':
                if self.ignore_first:
                    self._writer.writerow(self.header[1:])
                else:
                    self._writer.writerow(self.header)
                
        return self._writer
     
    @property
    def linewriter(self):
        '''Like writer, but does not write a header. '''
        if self._writer is None:
            import csv
            self.close()
            
            if self.exists:
                mode = 'a+'
            else:
                mode = 'w'
            
            if not os.path.exists(self.path):
                if not os.path.exists(os.path.dirname(self.path)):
                    os.makedirs(os.path.dirname(self.path))
            
            self.file = open(self.path, mode)
            self._writer = csv.writer(self.file)

        return self._writer
            
  
    @property
    def reader(self, mode='r'):
        '''Open a DictReader on the temp file. '''
        if self._reader is None:
            import csv
            self.close()
            self.file = open(self.path, mode, buffering=1*1024*1024)
            self._reader = csv.DictReader(self.file)
            
        return self._reader
       
    @property
    def linereader(self, mode='r', skip_header = True):
        '''Open a regular, list-oriented reader on the temp file
        '''
        if self._reader is None:
            import csv
            self.close()
            self.file = open(self.path, mode, buffering=1*1024*1024)
            self._reader = csv.reader(self.file)
            
        return self._reader
       
    @property 
    def path(self):
        return self._path

    @property
    def exists(self):
        return os.path.exists(self.path)
    
    def delete(self):
        self.close()
        if self.exists:
            os.remove(self.path)
    
    def close(self):
        if self.file:
            self.file.flush()
            self.file.close()
            self.file = None
            self._writer = None
            
            hk = self.table.name+'-'+str(self.suffix)
            if hk in self.db._tempfiles:
                del self.db._tempfiles[hk]
  
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self

class DbmFile(object):
    
    def __init__(self, bundle, db, table=None, suffix=None):

        self.bundle = bundle

        self.suffix = suffix

        self._table = table
        try:
            table_name = table.name
        except:
            table_name = table

        self._path = str(db.path)

        if table_name:
            self._path += '-'+table_name
            
        if suffix:
            self._path += '-'+suffix
            
        self._path += '.dbm'
       
            
        self._file = None
      
        
    @property
    def reader(self):
        self.close()
        self._file = anydbm.open(self._path, 'r')
        return self
   
    @property
    def writer(self):
        self.close()
        self._file = anydbm.open(self._path, 'c')
        return self
        
    def delete(self):
        
        if os.path.exists(self._path):
            os.remove(self._path)
        
        
    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    
    def __getitem__(self, key):
        return self._file[key]
        

    def __setitem__(self, key, val):
        #print key,'<-',val
        self._file[str(key)] =  str(val)
    

class DatabaseInterface(object):
    
    @property
    def name(self):  
        raise NotImplementedError() 
    
    @property 
    def path(self):
        raise NotImplementedError() 
   
    def exists(self):
        raise NotImplementedError() 
    
    def create(self):
        raise NotImplementedError() 
    
    def add_post_create(self, f):
        raise NotImplementedError() 
    
    def delete(self):
        raise NotImplementedError() 
    
    def open(self):
        raise NotImplementedError() 
    
    def close(self):
        raise NotImplementedError() 
    
    def inserter(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def updater(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def commit(self):
        raise NotImplementedError() 
   


class Database(DatabaseInterface):
    '''Represents a Sqlite database'''

    BUNDLE_DB_NAME = 'bundle'
    PROTO_SQL_FILE = 'support/configuration-sqlite.sql' # Stored in the databundles module. 
    EXTENSION = '.db'

    def __init__(self, bundle, base_path, post_create=None):   
        '''Initialize the a database object
        
        Args:
            bundle. a Bundle object
            
            base_path. Path to the database file. If None, uses the name of the
            bundle, in the bundle build director. 
            
            post_create. A function called during the create() method. has
            signature post_create(database)
       
        '''
        self.container = self.bundle = bundle 
        
        self._engine = None
        self._session = None
        self._connection = None
        
        # DB-API is needed to issue INSERT OR REPLACE type inserts. 
        self._dbapi_cursor = None
        self._dbapi_connection = None
        
        self._post_create = []
        
        if post_create:
            self.add_post_create.append(post_create)
        
    
        # For database bundles, where we have to pass in the whole file path
        base_path, ext = os.path.splitext(base_path)
        
        if ext and ext != self.EXTENSION:
            raise Exception("Bad extension to file: {}: {}".format(base_path, ext))
        
        self.base_path = base_path

      
        self._last_attach_name = None
        
        self._attachments = set()
        
        self._table_meta_cache = {}
        
        self._tempfiles = {}
        self._dbmfiles = {}
       
    @property
    def name(self):
        return Database.BUNDLE_DB_NAME

    @classmethod
    def make_path(cls, container):
        return container.path + cls.EXTENSION

    @property 
    def path(self):
        return self.make_path(self.container)
    
    def sub_dir(self, *args):
        return  self.container.sub_dir(*args)
      
    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''
        
        from sqlalchemy import MetaData   
        metadata = MetaData(bind=self.engine)

        return metadata
    
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
            self._engine = create_engine('sqlite:///'+self.path, echo=False) 
            #self._engine = create_engine('sqlite://') 
            from sqlalchemy import event
            event.listen(self._engine, 'connect', _pragma_on_connect)
             
        return self._engine

    @property
    def connection(self):
        '''Return an SqlAlchemy connection'''
        if not self._connection:
            self._connection = self.engine.connect()
            
        return self._connection
    
    def add_post_create(self, f):
        self._post_create.append(f)
    
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
        
    def tempfile(self, table, header=None, suffix=None, ignore_first=False):
        
        hk = (table,suffix)
    
        if hk not in self._tempfiles:
            self._tempfiles[hk] = TempFile(self.bundle, self, table, header=header, 
                                          suffix=suffix, ignore_first=ignore_first)
      
        return self._tempfiles[hk]

    def dbm(self,table=None, suffix=None):
        
        hk = (table,suffix)
    
        if hk not in self._dbmfiles:
            self._dbmfiles[hk] = DbmFile(self.bundle, self, table=table, suffix=suffix)
      
        return self._dbmfiles[hk]
   

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
   
    def exists(self):
        return os.path.exists( self.path)
    
    def is_empty(self):
        
        return not self.exists()
        
    
    def delete(self):
        
        try :
            os.remove(self.path)
        except:
            pass
        

    def inserter(self, table_or_name=None,**kwargs):

        if table_or_name is None and self.partition.table is not None:
            table_or_name = self.partition.table
      
        if isinstance(table_or_name, basestring):
            table_name = table_or_name
            if not table_name in self.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                t_meta.create_all(bind=self.engine)
                
                if not table_name in self.inspector.get_table_names():
                    raise Exception("Don't have table "+table_name)
            table = self.table(table_name)
            
        else:
            table = self.table(table_or_name.name)

        return ValueInserter(self.bundle, table , self,**kwargs)
        
    def updater(self, table_or_name=None,**kwargs):
      
        if table_or_name is None and self.partition.table is not None:
            table_or_name = self.partition.table
      
        if isinstance(table_or_name, basestring):
            table_name = table_or_name
            if not table_name in self.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                t_meta.create_all(bind=self.engine)
                
                if not table_name in self.inspector.get_table_names():
                    raise Exception("Don't have table "+table_name)
            table = self.table(table_name)
            
        else:
            table = self.table(table_or_name.name)
            
        
        return ValueUpdater(self.bundle, table , self,**kwargs)
        
    def load_sql(self, sql_file):
        import sqlite3
        conn = sqlite3.connect( self.path)
        f =  open(sql_file)
        sql =f.read().strip()
       
        conn.executescript(sql)
        
        f.close()
        
        conn.commit()
        
    def clean_table(self, table):

        if isinstance(table, basestring):
            self.connection.execute('DELETE FROM {} '.format(table))
        else:
            self.connection.execute('DELETE FROM {} '.format(table.name))
            
        self.commit()

        
    def load_tempfile(self, tempfile, table=None):
        '''Load a tempfile into the database. Uses the header line of the temp file
        for the column names '''
    
        if not tempfile.exists:
            self.bundle.log("Tempfile already deleted. Skipping")
            return
        
        lr = tempfile.linereader
        
        try:
            column_names = lr.next() # Get the header line. 
        except Exception as e:
            self.bundle.error("Failed to get header line from {} ".format(tempfile.path))
            raise e
 
        if table is None:
            table = tempfile.table
            
        try: # Table is either an object, or a string
            table_name = table.name
        except:
            table_name = table
 
       
       
        ins =  ("""INSERT INTO {table} ({columns}) VALUES ({values})"""
                            .format(
                                 table=table_name,
                                 columns =','.join(column_names),
                                 values = ','.join(['?' for c in column_names]) #@UnusedVariable
                            )
                         )
        
        try:

            # self.dbapi_connection.text_factory = lambda x: unicode(x, "utf-8", "ignore")
            #self.dbapi_connection.text_factory = lambda x: None if not x.strip() else x;
            
            self.create_table(table_name)
            
            if False: # For debugging some hash conflicts
                for row in lr:
                    print 'ROW', row
                    self.dbapi_cursor.execute(ins, row)
                    self.dbapi_connection.commit()
            else:
                self.dbapi_cursor.executemany(ins, lr)
                self.dbapi_connection.commit()
                
        except Exception as e:
            self.bundle.error("Failed to store tempfile "+tempfile.path)
            self.bundle.error("Insert code: "+ins)
            self.dbapi_connection.rollback()

            raise e
            
        self.dbapi_close()
    
    def create(self):
        
        """Create the database from the base SQL"""
        if not self.exists():    
            import databundles  #@UnresolvedImport
            from databundles.orm import Dataset
            from databundles.identity import Identity
            try:   
                script_str = os.path.join(os.path.dirname(databundles.__file__),
                                          Database.PROTO_SQL_FILE)
            except:
                # Not sure where to find pkg_resources, so this will probably
                # fail. 
                from pkg_resources import resource_string #@UnresolvedImport
                script_str = resource_string(databundles.__name__, Database.PROTO_SQL_FILE)
         
            dir_ = os.path.dirname(self.path);
            
            if not os.path.isdir(dir_):
                os.makedirs(dir_)
         
            self.load_sql(script_str)
            
            # Create the Dataset
            s =  self.session
            ds = Dataset(**self.bundle.config.identity)
            ds.name = Identity.name_str(ds)
            ds.vname = Identity.name_str(ds, use_revision=True)
            
            
            s.add(ds)
            s.commit()
 
            
            # call the post create function
            for f in self._post_create:
                f(self)
            
        return self
      
        
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
            t_meta.create_all(bind=self.engine)
            
            if not table_name in self.inspector.get_table_names():
                raise Exception("Don't have table "+table_name)
                   
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
      
        
    def clean(self):
        '''Remove all files generated by the build process'''
        os.remove(self.path)
        
    def query(self,*args, **kwargs):
        """Convience function for self.connection.execute()"""
        
        if isinstance(args[0], basestring):
            fd = { x:x for x in self._attachments }
        
            args = (args[0].format(**fd),) + args[1:]
            
        
        return self.connection.execute(*args, **kwargs)
        
        
    def attach(self,id_, name=None):
        """Attach another sqlite database to this one
        
        Args:
            id_ Itentifies the other database. May be a:
                Path to a database
                Identitfier object, for a undle or partition
                Datbase or PartitionDb object
                
            name. Name by which to attach the database. Uses a random
            name if None
        
        The method iwll also store the name of the attached database, which 
        will be used in copy_to() and copy_from() if a name is not provided
          
        Returns:
            name by whih the database was attached
                
        """
        from identity import Identity
        from partition import Partition
        from bundle import Bundle
    
        if isinstance(id_,basestring):
            #  Strings are path names
            path = id_
        elif isinstance(id_, Identity):
            path = id_.path
        elif isinstance(id_,Database):
            path = id_.path
        elif isinstance(id_,Partition):
            path = id_.database.path
        elif isinstance(id_,Bundle):
            path = id_.database.path
        else:
            raise Exception("Can't attach: Don't understand id_: {}".format(repr(id_)))
        
        if name is None:
            import random, string
            name =  ''.join(random.choice(string.letters) for i in xrange(10)) #@UnusedVariable
        
        q = """ATTACH DATABASE '{}' AS '{}' """.format(path, name)

    
        self.connection.execute(q)
           
        self._last_attach_name = name
        
        self._attachments.add(name)
        
        return name
        
    def detach(self, name=None):
        """Detach databases
        
        Args:
            name. Name of database to detach. If None, detatch all
            
        
        """
    
        if name is None:
            name = self._last_attach_name
    
        self.connection.execute("""DETACH DATABASE {} """.format(name))
    
        self._attachments.remove(name)
    
    
    
    def copy_from_attached(self, table, columns=None, name=None, 
                           on_conflict= 'ABORT', where=None):
        """ Copy from this database to an attached database
        
        Args:
            map_. a dict of k:v pairs for the values in this database to
            copy to the remote database. If None, copy all values
        
            name. The attach name of the other datbase, from self.attach()
        
            on_conflict. How conflicts should be handled
            
            where. An additional where clause for the copy. 
            
        """
        
        if name is None:
            name = self._last_attach_name
        
        f = {'db':name, 'on_conflict': on_conflict, 'from_columns':'*', 'to_columns':''}
        
        if isinstance(table,basestring):
            # Copy all fields between tables with the same name
            f['from_table']  = table
            f['to_table'] = table
    
        elif isinstance(table, tuple):
            # Copy all ields between two tables with different names
            f['from_table'] = table[0]
            f['to_table'] = table[1]
        else:
            raise Exception("Unknown table type "+str(type(table)))

        if columns is None:
            pass
        elif isinstance(columns, dict):
            f['from_columns'] = ','.join([ k for k,v in columns.items() ])
            f['to_columns'] =  '('+','.join([ v for k,v in columns.items() ])+')'
            
        q = """INSERT OR {on_conflict} INTO {to_table} {to_columns} 
               SELECT {from_columns} FROM {db}.{from_table}""".format(**f)
    
        if where is not None:
            q = q + " " + where.format(**f)
    
        self.connection.execute(q)
  

    def characterize(self, table, column):
        '''Return information about a column in a table'''
        raise NotImplemented


class PartitionDb(Database):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, bundle, partition, base_path, **kwargs):
        '''''' 
        
        super(PartitionDb, self).__init__(bundle, base_path, **kwargs)  
        self.container = self.partition = partition
    
    @property
    def name(self):
        return self.partition.name

    def inserter(self, table_or_name=None,**kwargs):
        
        if table_or_name is None and self.table:
            table_or_name = self.partition.identity.table

        return super(PartitionDb, self).inserter(table_or_name, **kwargs)
        
    
    def create(self, copy_tables = True):
        from databundles.orm import Dataset
        from databundles.orm import Table
        
        '''Like the create() for the bundle, but this one also copies
        the dataset and makes and entry for the partition '''
        
        if super(PartitionDb, self).create():
        
            # Copy the dataset record
            bdbs = self.bundle.database.session 
            s = self.session
            dataset = bdbs.query(Dataset).one()
            s.merge(dataset)
            s.commit()
            
            # Copy the partition record
            from databundles.orm import Partition as OrmPartition 
        
            orm_p = bdbs.query(OrmPartition).filter(
                            OrmPartition.id_ == self.partition.identity.id_).one()
            s.merge(orm_p)
          
            #Copy the tables and columns
            if copy_tables:
                if orm_p.t_id is not None:
                    table = bdbs.query(Table).filter(Table.id_ == orm_p.t_id).one()
                    s.merge(table)
                    for column in table.columns:
                        s.merge(column)
                else:
                    for table in dataset.tables:
                        s.merge(table)
                        for column in table.columns:
                            s.merge(column)
                
            s.commit()
                  
            # Create a config key to mark this as a partition
     
class GeoDb(PartitionDb):
    
    def __init__(self, bundle, partition, base_path, **kwargs):
        ''''''    
        super(GeoDb, self).__init__(bundle, partition, base_path, **kwargs)  

        #self.connection.execute("SELECT load_extension('libspatialite.dylib');")

        def load_spatialite(this):
            
            pass # SHould load the spatialite library into sqlite here. 

        self.add_post_create(load_spatialite)
   
    def inserter(self,  table = None, dest_srs=4326, source_srs=None):
        
        if table is None and self.partition.identity.table:
            table = self.partition.identity.table
        
        return FeatureInserter(self.partition,  table, dest_srs, source_srs)
   
class HdfDb(Hdf5File, DatabaseInterface):
    
    EXTENSION = '.hdf5'
    
    def __init__(self,  partition):
        self.partition = partition
        self.bundle = partition.bundle

        self.container = self.partition

        dir_ = os.path.dirname(self.path)
        if not os.path.exists(dir_):
            os.makedirs(dir_)

        super(HdfDb, self).__init__(self.path)  
   
    @classmethod
    def make_path(cls, container):
        return container.path + cls.EXTENSION

    @property 
    def path(self):
        return self.make_path(self.container)
   
    def is_empty(self):
        # If the file is open, it will exist, so we need to check for stuff inside. 
        return not self.keys()
   
    def add_post_create(self, f):
        pass
   
class BundleDb(Database):
    
    '''Represents the database version of a bundle that is installed in a library'''
    def __init__(self, path):

        super(BundleDb, self).__init__(None, path)  
        
def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys=ON')
    dbapi_con.execute('PRAGMA journal_mode = OFF')
    #dbapi_con.enable_load_extension(True)
    #dbapi_con.execute('PRAGMA synchronous = OFF')

    
def insert_or_ignore(table, columns):
    return  ("""INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})"""
                            .format(
                                 table=table,
                                 columns =','.join([c.name for c in columns ]),
                                 values = ','.join(['?' for c in columns]) #@UnusedVariable
                            )
                         )

