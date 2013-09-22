

import os
from ..identity import Identity
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
        d = dict(d.items() + bundle.identity.to_dict().items())

    if not 'format' in d:
        d['format'] = 'db'
      
      
    if d['format'] == 'geo':
        from geo import GeoPartitionIdentity
        return GeoPartitionIdentity(**d)
    
    elif d['format'] == 'hdf':
        from hdf import HdfPartitionIdentity
        return HdfPartitionIdentity(**d)
    
    elif d['format'] == 'csv':
        from csv import CsvPartitionIdentity
        return CsvPartitionIdentity(**d)
    
    elif d['format'] == 'db':
        from sqlite import SqlitePartitionIdentity
        return SqlitePartitionIdentity(**d)
    
    elif d['format'] == Identity.ANY:
        return PartitionIdentity(**d)
    
    else:
        raise ValueError("Unknown format in : '{}' ".format(d))

class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''
    
    is_bundle = False
    is_partition = True
    
    time = None
    space = None
    table = None
    grain = None
    format = None
    segment = None
    
    def __init__(self, *args, **kwargs):


        d = {}

        for arg in args:
            if isinstance(arg, Identity):
                d = arg.to_dict()
        
        d = dict(d.items() + kwargs.items())

        self.from_dict(d)
        
        #self.name # Trigger some errors immediately. 
            
    def from_dict(self,d):
        
        super(PartitionIdentity, self).from_dict(d)
        
        self.time = d.get('time',None)
        self.space = d.get('space',None)
        self.table = d.get('table',None)
        self.grain = d.get('grain',None)
        self.format = d.get('format',None)
        self.segment = d.get('segment',None)

        if self.id_ is not None and self.id_[0] != ObjectNumber.TYPE.PARTITION:
            self.id_ = None

       
    def to_dict(self, include_parent=True):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        if include_parent:
            d =  super(PartitionIdentity, self).to_dict()
        else:
            d = {}
        
        d['time'] = self.time
        d['space'] = self.space
        d['table'] = self.table
        d['grain'] = self.grain
        d['format'] = self.format
        d['segment'] = self.segment

        return { k:v for k,v in d.items() if v}
    
    def _partition_name_parts(self, use_format=False):
        import re
        
        parts = [self.table, self.space, self.time, self.grain, self.segment]
        
        if use_format:
            parts += [self.format]
        
        p =  [re.sub('[^\w\.]','_',str(s))  for s in filter(None, parts )]

        assert len(p) != 0, "No parts for partition name: {}".format(self.to_dict(include_parent=False))
           
        return p
       
    @property
    def partition_path(self):
        '''The extension of the partition path from the bundle path'''

        return os.path.join(*self._partition_name_parts())
       
    @property
    def path(self):
        '''The name is a form suitable for use in a filesystem'''
        id_path  = super(PartitionIdentity,self).path
  
        return os.path.join(id_path, self.partition_path)

    @property
    def name(self):
        """The name of the bundle, excluding the revision"""
        return '-'.join(self._name_parts(use_revision=False))+'.'+'.'.join(self._partition_name_parts())

    @property
    def vname(self):
        """The name of the bundle, excluding the revision"""
        return '-'.join(self._name_parts(use_revision=True))+'.'+'.'.join(self._partition_name_parts())

    @property
    def as_dataset(self):
        """Convert this identity to the identity of the correcsponding dataset. """
        
        on = ObjectNumber.parse(self.id_)
        d = self.to_dict()
        d['id'] = str(on.dataset)
        
        return  Identity(**d)

    
    @staticmethod
    def convert(arg, bundle=None):
        """Try to convert the argument to a PartitionIdentity"""
        from databundles.orm import Partition
             
             
        raise Exception("Use new_identity instead")   
                
        if isinstance(arg, Partition):
            identity = PartitionIdentity(**(arg.to_dict()))
        elif isinstance(arg, tuple):
            identity = PartitionIdentity(**(arg._asdict())) 
        elif isinstance(arg, PartitionIdentity):
            identity = arg
        elif isinstance(arg, PartitionNumber):
            if bundle is not None:
                partition = bundle.partitions.get(str(arg))
                if partition:
                    identity = partition.identity
                else:
                    identity = None
                    raise ValueError("Could not find partition number {} in bundle"
                                     .format(arg))
            else:
                raise Exception("Must specify a bundle to convert PartitionNumbers")
        elif isinstance(arg, basestring):
            try:
                id = ObjectNumber.parse(arg)
                raise NotImplementedError("Converting PartitionNumber strings")
            except:
                raise ValueError("Can't convert string '{}' to a arg identity"
                             .format(type(arg)))
        else:
            raise ValueError("Can't convert type {} to a arg identity"
                             .format(type(arg)))

        return identity

   

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
    def data(self):
        return self.record.data
    
    
    @property
    def table(self):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
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
        
        self._db_class = None
        self._database =  None

    @classmethod
    def init(cls, record): 
        record.format = cls.FORMAT

    @property
    def name(self):
        return self.identity.name
    
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''

        return self.bundle.sub_path(self.identity.partition_path)


    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        return  os.path.join(self.path,*args)

    @property
    def tables(self):
        return self.data.get('tables',[])

    @property
    def table(self):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
        
        table_spec = self.identity.table
        
        if table_spec is None:
            return None
        
        return self.bundle.schema.table(table_spec)


    def unset_database(self):
        '''Removes the database record from the object'''
        self._database = None
       
    @property
    def data(self):
        return self.record.data 
    
    
    def inserter(self, table_or_name=None,**kwargs):
        
        if not self.database.exists():
            self.create()

        return self.database.inserter(table_or_name,**kwargs)
    
    def delete(self):
        
        try:
  
            self.database.delete()
            self._database = None
            
            s = self.bundle.database.session
            s.delete(self.record)
            s.commit()
            
            self.record = None
            
        except:
            raise
        
        
    
        
