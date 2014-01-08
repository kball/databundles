

import os
from ..identity import Identity, Name, NameQuery
from ..identity import ObjectNumber, PartitionNumber



def new_partition(bundle, orm_partition, **kwargs):
    
    db_type = orm_partition.format

    if db_type == 'geo':
        from geo import GeoPartition
        return GeoPartition(bundle, orm_partition, **kwargs)
    
    elif db_type == 'hdf':
        from hdf import HdfPartition
        return HdfPartition(bundle, orm_partition, **kwargs)
    
    elif db_type == 'csv':
        from csv import CsvPartition
        return CsvPartition(bundle, orm_partition, **kwargs)
    
    elif db_type == 'db':
        from sqlite import SqlitePartition 
        return SqlitePartition(bundle, orm_partition, **kwargs)
    
    else:
        raise ValueError("Unknown format: '{}' ".format(db_type))

def new_identity(d, bundle=None):

    if bundle:
        d = dict(d.items() + bundle.identity.dict.items())

    if not 'format' in d:
        d['format'] = 'db'
      
      
    if d['format'] == 'geo':
        from geo import GeoPartitionIdentity
        return GeoPartitionIdentity.from_dict(d)
    
    elif d['format'] == 'hdf':
        from hdf import HdfPartitionIdentity
        return HdfPartitionIdentity.from_dict(d)
    
    elif d['format'] == 'csv':
        from csv import CsvPartitionIdentity
        return CsvPartitionIdentity.from_dict(d)
    
    elif d['format'] == 'db':
        from sqlite import SqlitePartitionIdentity
        return SqlitePartitionIdentity.from_dict(d)
    
    elif d['format'] == NameQuery.ANY:
        from ..identity import PartitionIdentity
        return PartitionIdentity.from_dict(d)
    
    else:
        raise ValueError("Unknown format in : '{}' ".format(d))


class PartitionInterface(object):

    @property
    def name(self):  raise NotImplementedError()
    
    def _path_parts(self): 
        raise NotImplementedError()
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''
        raise NotImplementedError()

    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        raise NotImplementedError()
    
    @property
    def database(self):  
        raise NotImplementedError()


    def unset_database(self):
        '''Removes the database record from the object'''
        raise NotImplementedError()
    

       
    @property
    def tables(self):  raise NotImplementedError() 

    def create(self):  raise NotImplementedError()
        
    def delete(self):  raise NotImplementedError()
        
    def inserter(self, table_or_name=None,**kwargs):  raise NotImplementedError()

    def updater(self, table_or_name=None,**kwargs):  raise NotImplementedError()

    def write_stats(self):  raise NotImplementedError()


class PartitionBase(PartitionInterface):

    def __init__(self, db, record, **kwargs):
        
        self.bundle = db
        self.record = record
        
        self.dataset = self.record.dataset
        self.identity = self.record.identity
        self.data = self.record.data
        self.table = self.get_table()
        
        #
        # These two values take refreshible fields out of the partition ORM record. 
        # Use these if you are getting DetatchedInstance errors like: 
        #    sqlalchemy.orm.exc.DetachedInstanceError: Instance <Table at 0x1077d5450> 
        #    is not bound to a Session; attribute refresh operation cannot proceed
        self.record_count = self.record.count
        
        self._db_class = None
        self._database =  None

    @classmethod
    def init(cls, record): 
        record.format = cls.FORMAT

    @property
    def name(self):
        return self.identity.name
    
    
    def get(self):
        '''Fetch this partition from the library or remote if it does not exist'''
        import os
        return self.bundle.library.get(self.identity.vid).partition

    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''

        return self.bundle.sub_path(self.identity.path)


    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        return  os.path.join(self.path,*args)

    @property
    def tables(self):
        return self.data.get('tables',[])


    def get_table(self, table_spec=None):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
        
        if not table_spec:
            table_spec = self.identity.table
            
            if table_spec is None:
                return None
            
        return self.bundle.schema.table(table_spec)


    def unset_database(self):
        '''Removes the database record from the object'''
        self._database = None
       

    
    def inserter(self, table_or_name=None,**kwargs):
        
        if not self.database.exists():
            self.create()

        return self.database.inserter(table_or_name,**kwargs)
    
    def delete(self):
        
        try:
  
            self.database.delete()
            self._database = None
            
            with self.bundle.session as s:
                # Reload the record into this session so we can delete it. 
                from ..orm import Partition
                r = s.query(Partition).get(self.record.vid)
                s.delete(r)

            self.record = None
            
        except:
            raise
        

    def dbm(self, suffix = None):
        '''Return a DBMDatabase replated to this partition'''
        
        from ..database.dbm import Dbm
        
        return Dbm(self.bundle, base_path=self.path, suffix=suffix)
        
        
    @property
    def help(self):
        """Returns a human readable string of useful information"""
        
        info = dict(self.identity.to_dict(clean=False).items())
        info['path'] = self.database.path
        info['tables'] = ','.join(self.tables)

        return """
------ Partition: {name} ------
id    : {id}
vid   : {vid}
name  : {name}
vname : {vname}
path  : {path}
table : {table}
tables: {tables}
time  : {time}
space : {space}
grain : {grain}
        """.format(**info)
        
