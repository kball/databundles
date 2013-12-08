"""Identity objects for constructing names for bundles and partitions, and 
Object Numbers for datasets, columns, partitions and tables. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path

def new_identity(d, bundle=None):
    """Create a new identity from a dict form """
    from partition import PartitionIdentity
    from partition import new_identity as p_new_identity
    
    on = ObjectNumber.parse(d.get('id'))
    
    if  on: 
        if isinstance(on, DatasetNumber):
            return Identity(**d)
        elif isinstance(on, PartitionNumber):

            return p_new_identity(d)
        else:
            raise ValueError("parameter was not  dataset nor partition id: {} ".format(d))

    elif bundle:
        return p_new_identity(d, bundle=bundle)
    
    elif set(['time','space','table','grain', 'format']).intersection(set(d.keys())):    
        return p_new_identity(d)
    
    else:
        return Identity(**d)
        try : return Identity(**d)
        except Exception as e:
            raise Exception("Failed for {}: {}".format(d, e))

class Identity(object):

    is_bundle = True
    is_partition = False

    NONE = '<none>'
    ANY = '<any>'

    PATH_EXTENSION = '.db'

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
        
        try: self.revision =  int(d.get('revision',1))
        except:  self.revision =  None


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
 
    def to_meta(self, md5=None, file=None):
        '''Return a dictionary of metadata, for use in the Remote api'''
        import json
        
        if not md5:
            if not file:
                raise ValueError("Must specify either file or md5")
        
            from util import md5_for_file
            
            md5 = md5_for_file(file)
        
        return {
                'id':self.id_, 
                'identity': json.dumps(self.to_dict()),
                'name':self.name, 
                'md5':md5}
 
    def id_string(self):
        return self.vid
 
    def clone(self):
        return self.__class__(**self.to_dict())
 
    def as_revision(self, revision):
        '''Clone and change the revision'''
 
        if revision < 0:
            revision = self.revision-1
            
        c = self.clone()
        
        c.revision = revision
        
        return c
        

    @property
    def vid(self):
        try:
            return str(ObjectNumber.parse(self.id_).rev(self.revision))
        except AttributeError:
            return None
 
 
    @property
    def vid_enc(self):
        '''vid, urlencoded'''
        import urllib
        return self.vid.replace('/','|')
 
    @property
    def creatorcode(self):
        return self._creatorcode(self)
    
    @staticmethod
    def _creatorcode(o):
        import hashlib
        # Create the creator code if it was not specified. 
        
        if isinstance(o.creator, basestring) and len(o.creator) == 4:
            return o.creator # It is already hashed. 
        
        if o.creator is None:
            raise ValueError('Got identity object with None for creator')
        
        return hashlib.sha1(o.creator).hexdigest()[0:4]
    
    #
    # Naming, paths and cache_keys
    #

    
    def _name_parts(self, use_revision=True):
        """Return the parts of the name as a list, for additional processing. """
        
        from databundles.dbexceptions import ConfigurationError
        nparts = [];
 
        try: 
            if self.source is None:
                raise ConfigurationError('Source is None ')  
            nparts.append(self.source)
        except Exception as e:
            raise ConfigurationError('Missing identity.source for {},  {} '.format(self.__dict__, e))  
  
        try: 
            if self.dataset is None:
                raise ConfigurationError('Dataset is None ')  
            nparts.append(str(self.dataset))
        except Exception as e:
            raise ConfigurationError('Missing identity.dataset: '+str(e))  
        
        try: 
            if self.subset is not None:
                nparts.append(str(self.subset))
        except Exception as e:
            pass
        
        try: 
            if self.variation is not None:
                nparts.append(str(self.variation))
        except Exception as e:
            pass
        
        try: 
            nparts.append(self.creatorcode)
        except AttributeError:
            # input object doesn't have 'creatorcode'
            nparts.append(Identity._creatorcode(self))
        except Exception as e:
            raise ConfigurationError('Missing identity.creatorcode: '+str(e))
   
        if use_revision:
            try: 
                nparts.append('r'+str(self.revision))
            except Exception as e:
                raise ConfigurationError('Missing identity.revision: '+str(e))  

        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in nparts]


    @property
    def name(self):
        """The name of the bundle, excluding the revision"""
        return '-'.join(self._name_parts(use_revision=False))

    @property
    def name_enc(self):
        """The name of the bundle, excluding the revision, encoded"""
        return self.name.replace('.','_')

    @property
    def vname(self):
        """The name of the bundle, including the revision"""
        return '-'.join(self._name_parts(use_revision = True if self.revision else False))


    @property
    def path(self):
        '''The name is a form suitable for use in a filesystem'''
        parts = self._name_parts(use_revision=True)
        source = parts.pop(0)
        
        return os.path.join(source, '-'.join(parts) )
  

    @property
    def source_path(self):
        '''The path of the bundle source. '''
        parts = self._name_parts(use_revision=False)
        source = parts.pop(0)
        
        return os.path.join(source, '-'.join(parts) ) 
  
  
    @property
    def cache_key(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path+self.PATH_EXTENSION
 
 

 
 
    @classmethod
    def parse_name(cls,input):
        '''Parse a name to return the Identity. Will discard the Partition parts. '''
        
        import re

        rep = re.compile(r'([\w\.]+-[^\.]+)(\.[\.\w\d]+)?')
        g = rep.match(input)
        if not g:
            raise ValueError('Could not parse name (initial regex failed): {}'.format(input))

        bundle = g.groups()[0]
        partition = g.groups()[1]

        parts = bundle.split('-')
        
        try:
            source = parts.pop(0)
            dataset = parts.pop(0)
        except IndexError:
            raise ValueError('Could not parse name: {}'.format(input))
            
        revision = None
        partition = None
        creator = None
        variation = None
        subset = None
            
        reo = re.compile(r'r(\d+)')

        for i,p in enumerate(parts): # Get the revision, which must be r + numbers
            if  reo.match(p):
                revision = parts.pop(i)

        # Getting into hackery...
        # If the last part is four digits and has numbers, it must be the creator code. 
        if re.search(r'[\dabcdef]{4}', parts[-1]) and len(parts[-1]) == 4:
            creator = parts.pop()

        if parts:
            subset = parts.pop(0)
            
        if parts:
            variation = parts.pop(0)            
        
        if len(parts):
            raise ValueError('Could not parse name (Parts left over): {}'.format(input))
        
        return Identity( source = source, dataset = dataset, subset = subset, 
                         variation =  variation, creator =  creator, revision =  revision);

       
    def __str__(self):
        return self.name
       

   
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

    TCMAXVAL = 62*62 -1; # maximum for table values. 
    CCMAXVAL = 62*62*62 -1; # maximum for column values. 
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
            column = int(ObjectNumber.base62_decode(input[-3:]))
            table = int(ObjectNumber.base62_decode(input[-5:-3]))
            dataset = int(ObjectNumber.base62_decode(input[1:-5]))
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

        column = int(column)

        if column > ObjectNumber.CCMAXVAL:
            raise ValueError("Value {} is too large ( max is {} ) ".format(column, ObjectNumber.TCMAXVAL))

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
                ObjectNumber.base62_encode(self.column).rjust(3,'0')+
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



