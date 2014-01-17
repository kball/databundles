

import os
from ..identity import Identity, Name, NameQuery
from ..identity import ObjectNumber, PartitionNumber, PartitionIdentity


def name_class_from_format_name(name):

    from geo import GeoPartitionName
    from hdf import HdfPartitionName
    from csv import CsvPartitionName
    from sqlite import SqlitePartitionName

    if not name:
        name = 'db'

    for pc in (GeoPartitionName, HdfPartitionName, CsvPartitionName, SqlitePartitionName ):
        if name == pc.format_name():
            return pc

    raise KeyError("Unknown format name: {}".format(name))



def partition_class_from_format_name(name):

    from geo import GeoPartition
    from hdf import HdfPartition
    from csv import CsvPartition
    from sqlite import SqlitePartition

    if not name:
        name = 'db'

    for pc in (GeoPartition, HdfPartition, CsvPartition, SqlitePartition ):
        if name == pc.format_name():
            return pc

    raise KeyError("Unknown format name: {}".format(name))


def identity_class_from_format_name(name):

    from geo import GeoPartitionIdentity
    from hdf import HdfPartitionIdentity
    from csv import CsvPartitionIdentity
    from sqlite import SqlitePartitionIdentity

    if not name:
        name = 'db'

    for ic in (GeoPartitionIdentity, HdfPartitionIdentity,
               CsvPartitionIdentity, SqlitePartitionIdentity ):
        if name == ic.format_name():
            return ic

    raise KeyError("Unknown format name: {}".format(name))

def new_partition(bundle, orm_partition, **kwargs):

    cls = partition_class_from_format_name(orm_partition.format)

    return cls(bundle, orm_partition, **kwargs)

def new_identity(d, bundle=None):

    if bundle:
        d = dict(d.items() + bundle.identity.dict.items())

    if not 'format' in d:
        d['format'] = 'db'

    format_name = d['format']

    ic = partition_class_from_format_name(format_name)

    return ic.from_dict(d)

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

    _db_class = None

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

        return self.bundle.sub_dir(self.identity.sub_path)  #+self._db_class.EXTENSION

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
        

    @classmethod
    def format_name(self):
        return self._id_class._name_class.FORMAT

    @classmethod
    def extension(self):
        return self._id_class._name_class.PATH_EXTENSION

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
        
