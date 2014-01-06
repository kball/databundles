"""Identity objects for constructing names for bundles and partitions, and 
Object Numbers for datasets, columns, partitions and tables. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path

def new_identity(d, bundle=None):
    """Create a new identity from a dict form """
    from partition import new_identity as p_new_identity
    
    on = ObjectNumber.parse(d.get('vid'))
    
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


class Name(object):
    '''The Name part of an identity ''' 


    PATH_EXTENSION = '.db'

    _name_parts = [('source',None,False),
                  ('dataset',None,False),
                  ('subset',None,True),
                  ('type',None,True),
                  ('part',None,True),
                  ('variation','orig',True),
                  # Semantic Version, different from Object Number revision, 
                  # which is an int. "Version" is the preferred name, 
                  # but 'revision' is in the databases schema. 
                  ('version',None,True)
                  ]

    def __init__(self, *args, **kwargs):
        
        for k,default, optional in self.name_parts:
            if optional:
                setattr(self,k, kwargs.get(k,default))
            else:
                setattr(self,k, kwargs.get(k))

        self.version = self._parse_version(self.version)

      
    def _parse_version(self,version):
        import semantic_version as sv  # @UnresolvedImport
        if version is not None and isinstance(version,basestring):

            if version == PartialName.ANY:
                pass
            elif version == PartialName.NONE:
                pass
            else:
                try: 
                    version = sv.Version(version)
                except ValueError:
                    try: version = sv.Spec(version)
                    except ValueError:
                        raise ValueError("Could not parse '{}' as a semantic version".format(version))        
                
        if not version:
            version = sv.Version('0.0.0')
                
        return version
                

    @property 
    def name_parts(self):
        return self._name_parts

    def clear_dict(self, d):
        return { k:v for k,v in d.items() if v}

    @property
    def dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        return self._dict(with_name=True)

    def _dict(self, with_name=True):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        d = dict([ (k, getattr(self, k)) for k,_, _ in self.name_parts ] )
        
        if with_name :
            d['name'] =  self.name
            try: d['vname'] = self.vname
            except ValueError: pass 
            
        return self.clear_dict(d) 


    @property
    def name(self):

        d = self._dict(with_name=False)
    
        return '-'.join([ d[k] for (k,_,_) in self.name_parts 
                         if k and d.get(k,False) and k != 'version'])
    
    @property
    def vname(self):
        if not self.version:
            raise ValueError("No version set")
        
        import semantic_version  # @UnresolvedImport
        
        if isinstance(self.version,semantic_version.Spec):
            return self.name+str(self.version)
        else:
            return self.name+'-'+str(self.version)
    

    def _path_join(self,names=None, excludes=None, sep=os.sep):

        d = self._dict(with_name=False)

        if isinstance(excludes,basestring):
            excludes = set([excludes])
            
        if not isinstance(excludes,set):
            excludes = set(excludes)            

        if not names:
            if not excludes:
                excludes = set([])
            
            names = set(k for k,_,_ in self.name_parts) - set(excludes)
        else:
            names = set(names)
        
        return sep.join(
                   [ str(d[k]) for (k,_,_) in self.name_parts 
                         if k and d.get(k,False) and k in (names-excludes)])  
           

    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''

        # Need to do this to ensure the function produces the
        # bundle path when called from subclasses
        names = [ k for k,_,_ in Name._name_parts]

        return os.path.join(
                self.source,
                self._path_join(names=names, excludes='source',sep='-')
             )
                
    
    @property
    def source_path(self):
        '''The name in a form suitable for use in a filesystem. 
        Excludes the revision'''
        return self._path_join(excludes=['version'])

    @property
    def cache_key(self):
        '''The name in a form suitable for use as a cache-key'''
        return self.path+self.PATH_EXTENSION

    def clone(self):
        return self.__class__(**self.dict)

    def version(self, revision):
        '''Clone and change the version'''

            
        c = self.clone()
        
        c.version = self.version = self._parse_version(self.version)
        
        return c

    def __str__(self):
        return self.name


class PartitionName(Name):
    '''A Partition Name'''

    time = None
    space = None
    table = None
    grain = None
    format = None
    segment = None

    _local_name_parts = [ 
                  ('table',None,True),
                  ('time',None,True),
                  ('space',None,True),
                  ('grain',None,True),
                  ('format',None,True),
                  ('segment',None,True)]

    _name_parts = ( Name._name_parts[0:-1] +
                  _local_name_parts +
                  Name._name_parts[-1:])

    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''
        
        local_names = [ k for k,_,_ in self._local_name_parts]

        parts = [super(PartitionName, self).path]
        
        if self.table:
            parts.append(self.table)
            

        l = []
        if self.time: l.append(self.time)
        if self.space: l.append(self.space)
        
        if l: parts.append('-'.join(l))
            
        
        l = []
        if self.grain: l.append(self.grain)
        if self.segment: l.append(self.segment)
        
        if l: parts.append('-'.join(l))
        
        # the format value is part of the file extension
        
        return os.path.join(*parts)

class PartialMixin(object):

    def clear_dict(self, d):
        return { k:v if v is not None else self.NONE for k,v in d.items() }

    NONE = '<none>'
    ANY = '<any>'


    def _dict(self, with_name=True):
        '''Returns the identity as a dict. values that are empty are removed'''

        d = dict([ (k, getattr(self, k)) for k,_, _ in self.name_parts ] )
            
        return self.clear_dict(d) 
    
    @property
    def name(self):
        raise NotImplementedError("Can't get a string name from a partial name")
    
    @property
    def vname(self):
        raise NotImplementedError("Can't get a string name from a partial name")

    @property
    def path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def source_path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def cache_key(self):
        raise NotImplementedError("Can't get a cache_key from a partial name")

class PartialName(PartialMixin, Name):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string 
    
    When a partial name is returned as a dict, parts that were not
    specified in the constructor have a value of '<any.', and parts that
    were specified as None have a value of '<none>'
    '''

    NONE = PartialMixin.NONE
    ANY = PartialMixin.ANY

    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''

        np =  [ (k,PartialMixin.ANY, True) 
                for k,_, _ in super(PartialName, self).name_parts ]
  
        return np
        

class PartialPartitionName(PartialMixin,PartitionName):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string '''

    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''
        return [ (k,PartialMixin.ANY, True) 
                for k,_, _ in super(PartialName, self).name_parts ]
 

class ObjectNumber(object):
    '''
    Static class for holding constants and static methods related 
    to object numbers
    '''
    
    # When a name is resolved to an ObjectNumber, orig can 
    # be set to the input value, which can be important, for instance, 
    # if the value's use depends on whether the user specified a version 
    # number, since all values are resolved to versioned ONs
    orig = None
    
    class _const:
        class ConstError(TypeError): pass
        def __setattr__(self,name,value):
            if self.__dict__.has_key(name):
                raise self.ConstError, "Can't rebind const(%s)"%name
            self.__dict__[name]=value

    TYPE=_const()
    TYPE.DATASET = 'd'
    TYPE.PARTITION = 'p'
    TYPE.TABLE ='t'
    TYPE.COLUMN = 'c'
    
    VERSION_SEP = ''
    
    DLEN=_const()
    
    DLEN.DATASET = (5,9)
    DLEN.PARTITION = 3
    DLEN.TABLE = 2
    DLEN.COLUMN = 3
    DLEN.REVISION = (0,3)
    
    # Because the dataset numebr can be 5 or 9 characters, 
    # And the revision is optional, the datasets ( and thus all 
    # other objects ) , can have several differnt lengths. We
    # Use these different lengths to determine what kinds of
    # fields to parse 
    # 's'-> short dataset, 'l'->long datset, 'r' -> has revision
    DATASET_LENGTHS = {5:(5,None),
                       8:(5,3),
                       9:(9,None),
                       12:(9,3)}
    
    # Length of the caracters that aren't the dataset and revisions
    NDS_LENGTH = {'d': 0,
                  'p': DLEN.PARTITION,
                  't': DLEN.TABLE,
                  'c': DLEN.TABLE+DLEN.COLUMN}
    
    SMALL_DS_MAX = 62**DLEN.DATASET[0] -1
    TCMAXVAL = 62**DLEN.TABLE -1; # maximum for table values. 
    CCMAXVAL = 62**DLEN.COLUMN -1; # maximum for column values. 
    PARTMAXVAL = 62*DLEN.PARTITION -1; # maximum for table and column values. 
     
    EPOCH = 1325376000 # Jan 1, 2012 in UNIX time

    @classmethod
    def parse(cls, input): #@ReservedAssignment
        '''Parse a string into one of the object number classes. '''
        
        if input is None:
            return None
        
        if not input:
            raise Exception("Didn't get input")

        if  isinstance(input, unicode):
            dataset = input.encode('ascii')
      
        type_ = input[0]
        input = input[1:]
        
        ds_lengths = cls.DATASET_LENGTHS[len(input)-cls.NDS_LENGTH[type_]]
        
        dataset = int(ObjectNumber.base62_decode(input[0:ds_lengths[0]]))
        
        if ds_lengths[1]: 
            i = len(input)-ds_lengths[1]
            revision = int(ObjectNumber.base62_decode(input[i:]))
            input = input[0:i] # remove the revision
        else:
            revision = None
            
        input = input[ds_lengths[0]:]
      
        if type_ == cls.TYPE.DATASET:
            return DatasetNumber(dataset, revision=revision)
        
        elif type_ == cls.TYPE.TABLE:   
            table = int(ObjectNumber.base62_decode(input))
            return TableNumber(DatasetNumber(dataset), table, revision=revision)
        
        elif type_ == cls.TYPE.PARTITION:
            partition = int(ObjectNumber.base62_decode(input))
            return PartitionNumber(DatasetNumber(dataset), partition, revision=revision)   
                   
        elif type_ == cls.TYPE.COLUMN:     
            table = int(ObjectNumber.base62_decode(input[0:cls.DLEN.TABLE]))
            column = int(ObjectNumber.base62_decode(input[cls.DLEN.TABLE:]))

            return ColumnNumber(TableNumber(DatasetNumber(dataset), table), column, revision=revision)
        
        else:
            raise ValueError('Unknow type character: '+input[0]+ ' in '+str(input))
       

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

    @classmethod
    def _rev_str(cls, revision):

        return (ObjectNumber.base62_encode(revision).rjust(cls.DLEN.REVISION[1],'0') 
                if revision else '')

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
        
    @classmethod
    def _ds_str(cls, dataset):
        
        if isinstance(dataset, DatasetNumber):
            dataset = dataset.dataset
        
        return (ObjectNumber.base62_encode(dataset).rjust(cls.DLEN.DATASET[0],'0') 
                 if dataset < cls.SMALL_DS_MAX
                 else ObjectNumber.base62_encode(dataset).rjust(cls.DLEN.DATASET[1],'0') )

    def __str__(self):        
        return (ObjectNumber.TYPE.DATASET+
                self._ds_str(self.dataset)+
                ObjectNumber._rev_str(self.revision))
           
 

 

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
                DatasetNumber._ds_str(self.dataset)+
                ObjectNumber.base62_encode(self.table).rjust(self.DLEN.TABLE,'0')+
                ObjectNumber._rev_str(self.revision))
                  
         
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
                DatasetNumber._ds_str(self.dataset)+
                ObjectNumber.base62_encode(self.table.table).rjust(self.DLEN.TABLE,'0')+
                ObjectNumber.base62_encode(self.column).rjust(self.DLEN.COLUMN,'0')+
                ObjectNumber._rev_str(self.revision)
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
                DatasetNumber._ds_str(self.dataset)+
                ObjectNumber.base62_encode(self.partition).rjust(self.DLEN.PARTITION,'0')+
                ObjectNumber._rev_str(self.revision))


   
class Identity(object):
    '''Identities represent the defining set of information about a 
    bundle or a partition. Only the vid is actually required to 
    uniquely identify a bundle or partition, but the identity is also
    used for generating unique names and for finding bundles and partitions. '''
    
    
    is_bundle = True
    is_partition = False

    _on = None
    _name = None

    def __init__(self, name, object_number):
        
        self._on = object_number
        self._name = name

        self._name.version.patch = self._on.revision

    @classmethod
    def from_dict(cls, d):
        name = Name(**d)
        
        if 'id' in d and 'revision' in d:
            # The vid should be constructed from the id and the revision
            on = (ObjectNumber.parse(d['id']).rev(d['revision']))
        elif 'vid' in d:
            on = ObjectNumber.parse(d['vid'])
        else:
            raise ValueError("Must have id and revision, or vid")
                  
        return cls(name, on)

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


    #
    # Naming, paths and cache_keys
    #

    @property
    def on(self):
        '''Return the object number obect'''
        return self._on

    @property
    def id_(self):
        '''String version of the object number, without a revision'''
        
        return str(self._on.rev(None))

    @property
    def vid(self):
        '''String version of the object number'''
        return str(self._on)
   
    @property
    def name(self):
        """The name of the bundle, excluding the revision"""
        return self._name

    @property
    def vname(self):
        ''' '''
        return self._name.vname
 
    @property
    def fqname(self):
        """The fully qualified name, the versioned name and the
        vid. This is the same as str(self)"""
        return str(self)
 
    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''
        
        return self._name.path
    
    @property
    def source_path(self):
        '''The name in a form suitable for use in a filesystem. 
        Excludes the revision'''
        return self._name.source_path

    @property
    def cache_key(self):
        '''The name in a form suitable for use as a cache-key'''
        return self._name.cache_key
 
    def __str__(self):
        return self._name.vname+'~'+self.vid
       

class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''
    

    @property
    def as_dataset(self):
        """Convert this identity to the identity of the corresponding dataset. """
        
        on = ObjectNumber.parse(self.id_)
        d = self.to_dict()
        d['id'] = str(on.dataset)
        
        return  Identity(**d)




class Resolver(object):
    
    def resolve(self,s):
        pass
    
    
    def parse(self,s):
        
        name, id = s.split('--')
    
        # If the version
    

def make_resolver(library=None, bundle=None):
    '''Return a resolver object constructed on a library or bundle '''





