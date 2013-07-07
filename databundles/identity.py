"""Identity objects for constructing names for bundles and partitions, and 
Object Numbers for datasets, columns, partitions and tables. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path

def new_identity(d, bundle=None):
    """Create a new identity from a dict form """
    on = ObjectNumber.parse(d.get('id'))
    
    if  on: 
        if isinstance(on, DatasetNumber):
            return Identity(**d)
        elif isinstance(on, PartitionNumber):
            return PartitionIdentity(**d)
        else:
            raise ValueError("parameter was not  dataset nor partition id: {} ".format(d))

    elif bundle: 
        return PartitionIdentity(bundle.identity, **d)
    elif set(['time','space','table','grain', 'format']).intersection(set(d.keys())):

        try : return PartitionIdentity(**d)
        except Exception as e:
            raise Exception("Failed for {}: {}".format(d, e))
    
    else:
        return Identity(**d)
        try : return Identity(**d)
        except Exception as e:
            raise Exception("Failed for {}: {}".format(d, e))
        
    


class Identity(object):

    is_bundle = True
    is_partition = False

    def __init__(self, *args, **kwargs):
        self.from_dict(kwargs)
   
        self.name # Will trigger errors if anything is wrong
 
    def from_dict(self,d):
        self.id_ = d.get('id', d.get('id_'))
        self.source = d.get('source')
        self.dataset =  d.get('dataset')
        self.subset =  d.get('subset',None)
        self.variation =  d.get('variation','orig')
        self.creator =  d.get('creator')
        self.revision =  int(d.get('revision',1))

    def to_dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        d =  {
             'id':self.id_,
             'vid': self.vid, 
             'source':self.source,
             'dataset':self.dataset,
             'subset':self.subset,
             'variation':self.variation,
             'creator':self.creator,
             'revision':self.revision,
             'name' : self.name,
             'vname' : self.vname
             }

        return { k:v for k,v in d.items() if v}
 
    @property
    def vid(self):
        try:
            return str(ObjectNumber.parse(self.id_).rev(self.revision))
        except AttributeError:
            return None
 
    @property
    def creatorcode(self):
        return self._creatorcode(self)
    
    @staticmethod
    def _creatorcode(o):
        import hashlib
        # Create the creator code if it was not specified. 
        
        if o.creator is None:
            raise ValueError('Got identity object with None for creator')
        
        return hashlib.sha1(o.creator).hexdigest()[0:4]
           
    @property
    def name(self):
        """The name of the bundle, excluding the revision"""
        return self.name_str(self, use_revision=False)
 
    @property
    def vname(self):
        """The name of the bundle, including the revision"""
        return self.name_str(self, use_revision=True)
    
    @property
    def path(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path_str(self)
  
    @property
    def cache_key(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path_str(self)+".db"
    
    @classmethod
    def path_str(cls,o=None):
        '''Return the path name for this bundle'''

        parts = cls.name_parts(o)
        source = parts.pop(0)
        
        return os.path.join(source, '-'.join(parts) )
    
    @classmethod
    def name_str(cls,o=None, use_revision=False):
        
        return '-'.join(cls.name_parts(o,use_revision=use_revision))
    
    @staticmethod
    def name_parts(o=None, use_revision=True):
        """Return the parts of the name as a list, for additional processing. """
        from databundles.dbexceptions import ConfigurationError
        name_parts = [];
    
     
        if o is None:
            raise ConfigurationError('name_parts must be given an object')  

        try: 
            if o.source is None:
                raise ConfigurationError('Source is None ')  
            name_parts.append(o.source)
        except Exception as e:
            raise ConfigurationError('Missing identity.source for {},  {} '.format(o.__dict__, e))  
  
        try: 
            if o.dataset is None:
                raise ConfigurationError('Dataset is None ')  
            name_parts.append(str(o.dataset))
        except Exception as e:
            raise ConfigurationError('Missing identity.dataset: '+str(e))  
        
        try: 
            if o.subset is not None:
                name_parts.append(str(o.subset))
        except Exception as e:
            pass
        
        try: 
            if o.variation is not None:
                name_parts.append(str(o.variation))
        except Exception as e:
            pass
        
        try: 
            name_parts.append(o.creatorcode)
        except AttributeError:
            # input object doesn't have 'creatorcode'
            name_parts.append(Identity._creatorcode(o))
        except Exception as e:
            raise ConfigurationError('Missing identity.creatorcode: '+str(e))
   
        if use_revision:
            try: 
                name_parts.append('r'+str(o.revision))
            except Exception as e:
                raise ConfigurationError('Missing identity.revision: '+str(e))  

        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in name_parts]
       
       
    def __str__(self):
        return self.name
       

class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''
    
    is_bundle = False
    is_partition = True
    
    time = None
    space = None
    table = None
    grain = None
    format = None
    
    def __init__(self, *args, **kwargs):

        d = {}

        for arg in args:
            if isinstance(arg, Identity):
                d = arg.to_dict()
       
    
        d = dict(d.items() + kwargs.items())
    
        self.from_dict(d)
        
        self.name # Trigger some errors immediately. 
            
    def from_dict(self,d):
        
        super(PartitionIdentity, self).from_dict(d)
        
        self.time = d.get('time',None)
        self.space = d.get('space',None)
        self.table = d.get('table',None)
        self.grain = d.get('grain',None)
        self.format = d.get('format',None)
    
        if self.id_ is not None and self.id_[0] != ObjectNumber.TYPE.PARTITION:
            self.id_ = None

       
    def to_dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        d =  super(PartitionIdentity, self).to_dict()
        
        d['time'] = self.time
        d['space'] = self.space
        d['table'] = self.table
        d['grain'] = self.grain
        d['format'] = self.format

        return { k:v for k,v in d.items() if v}
    
    
    @classmethod
    def path_str(cls,o=None):
        '''Return the path name for this bundle'''
        import re
        
        id_path = Identity.path_str(o)

        # HACK HACK HACK!
        # The table,space,time,grain order must match up with Partition._path_parts
        partition_parts = [re.sub('[^\w\.]','_',str(s))
                         for s in filter(None, [o.table, o.space, o.time, o.grain, o.format])]
    
       
        return  os.path.join(id_path ,  *partition_parts )
        
    
    @classmethod
    def name_str(cls,o=None, use_revision=False):
        
        return '-'.join(cls.name_parts(o, use_revision))
    
    @classmethod
    def name_parts(cls,o=None, use_revision=False):
        import re
       
        parts = Identity.name_parts(o)
    
        rev = parts.pop()
        # HACK HACK HACK!
        # The table,space,time,grain order must match up with Partition._path_parts and self._path_pats
        partition_component = '.'.join([re.sub('[^\w\.]','_',str(s))
                         for s in filter(None, [o.table, o.space, o.time, o.grain, o.format])])
        
        parts.append(partition_component)
        
        if use_revision:
            parts.append(rev)
        
        return parts
    
    @property
    def cache_key(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path_str(self)+".db"
    
    
    
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

   
class GeoPartitionIdentity(PartitionIdentity):
    pass
  
class HdfPartitionIdentity(PartitionIdentity):
    @property
    def cache_key(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path_str(self)+".hdf5"

   
class ObjectNumber(object):
    '''
    Static class for holding constants and static methods related 
    to object numbers
    '''
    class _const:
        class ConstError(TypeError): pass
        def __setattr__(self,name,value):
            if self.__dict__.has_key(name):
                raise self.ConstError, "Can't rebind const(%s)"%name
            self.__dict__[name]=value

    TYPE=_const()
    TYPE.DATASET = 'a'
    TYPE.PARTITION = 'b'
    TYPE.TABLE ='c'
    TYPE.COLUMN = 'd'

    TCMAXVAL = 62*62 -1; # maximum for table and column values. 
    PARTMAXVAL = 62*62*62 -1; # maximum for table and column values. 
     
    EPOCH = 1325376000 # Jan 1, 2012 in UNIX time

    @classmethod
    def parse(cls, input): #@ReservedAssignment
        '''Parse a string into one of the object number classes. '''
        

        if input is None:
            return None
        
        if not input:
            raise Exception("Didn't get input")

        if '/' in input: # The string has a revision
            revision = int(ObjectNumber.base62_decode(input[-3:]))
            input = input[:-4]
        else:
            revision = None

        
        if  isinstance(input, unicode):
            dataset = input.encode('ascii')
      
        if input[0] == cls.TYPE.DATASET:
            dataset = int(ObjectNumber.base62_decode(input[1:]))
            return DatasetNumber(dataset, revision=revision)
        elif input[0] == cls.TYPE.TABLE:   
            table = int(ObjectNumber.base62_decode(input[-2:]))
            dataset = int(ObjectNumber.base62_decode(input[1:-2]))
            return TableNumber(DatasetNumber(dataset), table, revision=revision)
        elif input[0] == cls.TYPE.PARTITION:
            partition = int(ObjectNumber.base62_decode(input[-3:]))
            dataset = int(ObjectNumber.base62_decode(input[1:-3]))  
            return PartitionNumber(DatasetNumber(dataset), partition, revision=revision)              
        elif input[0] == cls.TYPE.COLUMN:       
            column = int(ObjectNumber.base62_decode(input[-2:]))
            table = int(ObjectNumber.base62_decode(input[-4:-2]))
            dataset = int(ObjectNumber.base62_decode(input[1:-4]))
            return ColumnNumber(TableNumber(DatasetNumber(dataset), table), column, revision=revision)
        else:
            raise ValueError('Unknow type character: '+input[0]+ ' in '+str(input))
       
    
    def __init__(self, primary, suffix=None):
        '''
        Constructor
        '''
        
        # If the primary is the same as this class, it is a copy constructor
        if isinstance(primary, self.__class__) and suffix is None:
            pass
        
        else:
            self.primary = primary
            self.suffix = suffix
    
    
  
    @classmethod
    def base62_encode(cls, num):
        """Encode a number in Base X
    
        `num`: The number to encode
        `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        if (num == 0):
            return alphabet[0]
        arr = []
        base = len(alphabet)
        while num:
            rem = num % base
            num = num // base
            arr.append(alphabet[rem])
        arr.reverse()
        return ''.join(arr)

    @classmethod
    def base62_decode(cls,string):
        """Decode a Base X encoded string into the number
    
        Arguments:
        - `string`: The encoded string
        - `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        base = len(alphabet)
        strlen = len(string)
        num = 0
    
        idx = 0
        for char in string:
            power = (strlen - (idx + 1))
            num += alphabet.index(char) * (base ** power)
            idx += 1
    
        return num

    def rev(self, i):
        '''Return a clone with a different revision'''
        from copy import copy
        on =  copy(self)
        on.revision = i
        return on


    def __eq__(self, other):
        return str(self) == str(other)

class DatasetNumber(ObjectNumber):
    '''An identifier for a dataset'''
    def __init__(self, dataset=None, revision=None):
        '''
        Constructor
        '''
      
        if dataset is None:
            import time
            dataset = int(time.time())
    
        # For Datasets, integer values are time 
        # This calc is OK until 31 Dec 2053 00:00:00 GMT
        if dataset > ObjectNumber.EPOCH:
            dataset = dataset - ObjectNumber.EPOCH
          
        self.dataset = dataset
        self.revision = revision
        

    def __str__(self):        
        return (ObjectNumber.TYPE.DATASET+
                ObjectNumber.base62_encode(self.dataset)+
                ('/'+ObjectNumber.base62_encode(self.revision).rjust(3,'0') if self.revision else '')
                )
           
 

 

class TableNumber(ObjectNumber):
    '''An identifier for a table'''
    def __init__(self, dataset, table, revision=None):
        if not isinstance(dataset, DatasetNumber):
            raise ValueError("Constructor requires a DatasetNumber")

        if table > ObjectNumber.TCMAXVAL:
            raise ValueError("Value is too large")


        self.dataset = dataset
        self.table = table
        self.revision = revision
        
        if not self.revision and dataset.revision:
            self.revision = dataset.revision
        
        
         
    def __str__(self):        
        return (ObjectNumber.TYPE.TABLE+
                ObjectNumber.base62_encode(self.dataset.dataset)+
                ObjectNumber.base62_encode(self.table).rjust(2,'0')+
                ('/'+ObjectNumber.base62_encode(self.revision).rjust(3,'0') if self.revision else ''))
                  
         
class ColumnNumber(ObjectNumber):
    '''An identifier for a column'''
    def __init__(self, table, column, revision=None):
        if not isinstance(table, TableNumber):
            raise ValueError("Constructor requires a TableNumber. got: "+str(type(table)))

        if column > ObjectNumber.TCMAXVAL:
            raise ValueError("Value is too large")

        self.table = table
        self.column = column
        self.revision = revision
   
        if not self.revision and table.revision:
            self.revision = table.revision
             
   
   
    @property
    def dataset(self):
        '''Return the dataset number for ths partition '''
        return self.table.dataset
         
         
         
    def __str__(self):        
        return (ObjectNumber.TYPE.COLUMN+
                ObjectNumber.base62_encode(self.table.dataset.dataset)+
                ObjectNumber.base62_encode(self.table.table).rjust(2,'0')+
                ObjectNumber.base62_encode(self.column).rjust(2,'0')+
                ('/'+ObjectNumber.base62_encode(self.revision).rjust(3,'0') if self.revision else '')
                )
           

class PartitionNumber(ObjectNumber):
    '''An identifier for a partition'''
    def __init__(self, dataset, partition, revision=None):
        '''
        Arguments:
        dataset -- Must be a DatasetNumber
        partition -- an integer, from 0 to 62^3
        '''
        if not isinstance(dataset, DatasetNumber):
            raise ValueError("Constructor requires a DatasetNumber")

        if partition > ObjectNumber.PARTMAXVAL:
            raise ValueError("Value is too large")

        self.dataset = dataset
        self.partition = partition
        self.revision = revision

        if not self.revision and dataset.revision:
            self.revision = dataset.revision
        
    def __str__(self):        
        return (ObjectNumber.TYPE.PARTITION+
                ObjectNumber.base62_encode(self.dataset.dataset)+
                ObjectNumber.base62_encode(self.partition).rjust(3,'0')+
                ('/'+ObjectNumber.base62_encode(self.revision).rjust(3,'0') if self.revision else ''))



