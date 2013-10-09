"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from .sqlite import SqliteDatabase #@UnresolvedImport
from .relational import RelationalPartitionDatabaseMixin, RelationalDatabase #@UnresolvedImport
from inserter import ValueInserter, ValueUpdater

class PartitionDb(SqliteDatabase, RelationalPartitionDatabaseMixin):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, bundle, partition, base_path, memory = False, **kwargs):
        '''''' 

        RelationalPartitionDatabaseMixin._init(self,bundle,partition)
        self.memory = memory
    
        super(PartitionDb, self).__init__(base_path, memory=self.memory, **kwargs)  

        self._session = None

    def query(self,*args, **kwargs):
        """Convience function for self.connection.execute()"""
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import QueryError
        
        if isinstance(args[0], basestring):
            fd = { x:x for x in self._attachments }
        
            args = (args[0].format(**fd),) + args[1:]
            
        try:
            return self.connection.execute(*args, **kwargs)
        except OperationalError as e:
            raise QueryError("Error while executing {} in database {} ({}): {}".format(args, self.dsn, type(self), e.message))
        

    def inserter(self, table_or_name=None,**kwargs):

        if table_or_name is None and self.partition.table is not None:
            table_or_name = self.partition.table
      
        if isinstance(table_or_name, basestring):
            table_name = table_or_name
            if not table_name in self.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                table.create(bind=self.engine)
                
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
                table.create(bind=self.engine)
                
                if not table_name in self.inspector.get_table_names():
                    raise Exception("Don't have table "+table_name)
            table = self.table(table_name)
            
        else:
            table = self.table(table_or_name.name)
            
        return ValueUpdater(self.bundle, table , self,**kwargs)

    def create(self):
        from databundles.orm import Dataset

        '''Like the create() for the bundle, but this one also copies
        the dataset and makes and entry for the partition '''
        
        
        self.require_path()
        
        if RelationalDatabase._create(self):
            self.post_create()
              
    @property
    def engine(self):
        return self._get_engine(_on_connect_partition)
                  
    # DEPRECATED! Should use the session_context instead
    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        if not self._session:
            Session = sessionmaker(bind=self.engine,autocommit=False)
            self._session =  Session()
            
        return self._session


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
        from ..identity import Identity
        from ..partition import PartitionInterface
        from ..bundle import Bundle
    
        if isinstance(id_,basestring):
            #  Strings are path names
            path = id_
        elif isinstance(id_, Identity):
            path = id_.path
        elif isinstance(id_,PartitionDb):
            path = id_.path
        elif isinstance(id_,PartitionInterface):
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
   
def _on_connect_partition(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')
    dbapi_con.execute('PRAGMA journal_mode = WAL')
    #dbapi_con.execute('PRAGMA synchronous = OFF')
    #dbapi_con.enable_load_extension(True)
