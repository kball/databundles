"""Identity objects for constructing names for bundles and partitions, and 
Object Numbers for datasets, columns, partitions and tables. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
from semantic_version import Version 
from util.typecheck import returns, accepts

class Name(object):
    '''The Name part of an identity ''' 


    PATH_EXTENSION = '.db'

    # Name, Default Value, Is Optional
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

    source = None
    dataset = None
    subset = None
    type = None
    part = None
    variation = None
    version = None

    def __init__(self, *args, **kwargs):

        """

        :param args:
        :param kwargs:
        """
        for k,default, optional in self.name_parts:
            if optional:
                setattr(self,k, kwargs.get(k,default))
            else:
                setattr(self,k, kwargs.get(k))


        self.version = self._parse_version(self.version)

        self.is_valid()



    def is_valid(self):

        """


        :raise ValueError:
        """
        for k, _, optional in self.name_parts:
            if not optional and not bool(getattr(self,k)):
                raise ValueError("Name requires field '{}' to have a value"
                                 .format(k))
        
    @returns(str, debug=2)  
    def _parse_version(self,version):
        import semantic_version as sv  # @UnresolvedImport
        
        if version is not None and isinstance(version,basestring):

            if version == NameQuery.ANY:
                pass
            elif version == NameQuery.NONE:
                pass
            else:
                try: 
                    version = str(sv.Version(version))
                except ValueError:
                    try: version = str(sv.Spec(version))
                    except ValueError:
                        raise ValueError("Could not parse '{}' as a semantic version".format(version))        
                
        if not version:
            version = str(sv.Version('0.0.0'))
                
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
        '''String version of the name, excluding the version, and
        excluding the format, if the format is 'db' '''
        
        d = self._dict(with_name=False)
    
        return '-'.join([ d[k] for (k,_,_) in self.name_parts 
                         if k and d.get(k,False) 
                         and k != 'version'
                         and not (k == 'format' and d[k] == 'db') ])
    
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
        # Need to do this to ensure the function produces the
        # bundle path when called from subclasses
        names = [ k for k,_,_ in Name._name_parts]

        return os.path.join(
                self.source,
                self._path_join(names=names,
                                excludes=['source','version'],sep='-')
             )
                

    @property
    def cache_key(self):
        '''The name in a form suitable for use as a cache-key'''
        return self.path+self.PATH_EXTENSION

    def clone(self):
        return self.__class__(**self.dict)

    def ver(self, revision):
        '''Clone and change the version'''

            
        c = self.clone()
        
        c.version =  self._parse_version(self.version)
        
        return c

    def type_is_compatible(self, o):

        if type(o) != DatasetNumber:
            return False
        else:
            return True
      
    # The name always stores the version number as a string, so these
    # convenience functions make it easier to update specific parts
    @property
    def version_minor(self): return Version(self.version).minor
    
    @version_minor.setter
    def version_minor(self, value):  
        v = Version(self.version)
        v.minor = value
        self.version = str(v)
 
    @property
    def version_major(self): return Version(self.version).minor
    
    @version_major.setter
    def version_major(self, value):  
        v = Version(self.version)
        v.major = value
        self.version = str(v)
    
    @property
    def version_patch(self): return Version(self.version).patch
    
    @version_patch.setter
    def version_patch(self, value):  
        v = Version(self.version)
        v.patch = value
        self.version = str(v)
 

    @property
    def version_build(self): return Version(self.version).build
    
    @version_build.setter
    def version_build(self, value):  
        v = Version(self.version)
        v.build = value
        self.version = str(v)
 
    
    def __str__(self):
        return self.name

class PartialPartitionName(Name):
    '''For specifying a PartitionName within the context of a bundle. 
    '''
    
    time = None
    space = None
    table = None
    grain = None
    format = None
    segment = None

    _name_parts = [ 
                  ('table',None,True),
                  ('time',None,True),
                  ('space',None,True),
                  ('grain',None,True),
                  ('format',None,True),
                  ('segment',None,True)]

    def promote(self, name):
        '''Promote to a PartitionName by combining with 
        a bundle Name'''

        
        return PartitionName(**dict(name.dict.items() +
                                    self.dict.items() ))
        

    def is_valid(self): pass
    
class PartitionName(PartialPartitionName, Name):
    '''A Partition Name'''


    _name_parts = ( Name._name_parts[0:-1] + 
                  PartialPartitionName._name_parts +
                  Name._name_parts[-1:])

    def _local_parts(self):
        
        parts = []
        
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
        
        return parts
 
 
    @property
    def name(self):

        d = self._dict(with_name=False)

        return '-'.join([ d[k] for (k,_,_) in self.name_parts 
                         if k and d.get(k,False) 
                         and k != 'version'
                         and (k != 'format' or str(d[k]) != 'db') ])
    
        
    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''

        return os.path.join(*(
                              [super(PartitionName, self).path]+
                              self._local_parts())
                            )

    @property
    def source_path(self):
        '''The path of the bundle source. Includes the revision. '''

        return os.path.join(*(
                              [super(PartitionName, self).source_path]+
                              self._local_parts()) 
                            )

    def type_is_compatible(self, o):
        
        if type(o) != PartitionNumber:
            return False
        else:
            return True
        

class PartialMixin(object):

    use_clear_dict = True

    def clear_dict(self, d):
        if self.use_clear_dict:
            return { k:v if v is not None else self.NONE for k,v in d.items() }
        else:
            return d

    NONE = '<none>'
    ANY = '<any>'


    def _dict(self, with_name=True):
        '''Returns the identity as a dict. values that are empty are removed'''

        d = dict([ (k, getattr(self, k)) for k,_, _ in self.name_parts ] )
            
        return self.clear_dict(d) 
    
    def with_none(self):
        '''Convert the NameQuery.NONE to None. This is needed because on the
        kwargs list, a None value means the field is not specified, which 
        equates to ANY. The _find_orm() routine, however, is easier to write if
        the NONE value is actually None 
        
        Returns a clone of the origin, with NONE converted to None
        '''
    
        n = self.clone()
    
        for k,_,_ in n.name_parts:
            
            if getattr(n,k) == n.NONE:
                delattr(n,k)

        n.use_clear_dict = False
    
        return n
    
    def is_valid(self):
        return True


    @property
    def path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def source_path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def cache_key(self):
        raise NotImplementedError("Can't get a cache_key from a partial name")

class NameQuery(PartialMixin, Name):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string 
    
    When a partial name is returned as a dict, parts that were not
    specified in the constructor have a value of '<any.', and parts that
    were specified as None have a value of '<none>'
    '''

    NONE = PartialMixin.NONE
    ANY = PartialMixin.ANY

    # These are valid values for a name query
    name = None
    vname = None
    fqname = None


    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''
        
        default = PartialMixin.ANY

        np =  ([ (k,default, True) 
                for k,_, _ in super(NameQuery, self).name_parts ]
               + 
               [('name',default,True),
                ('vname',default,True),
                ('fqname',default,True)]
               )
  
        return np
        

class PartitionNameQuery(PartialMixin,PartitionName):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string '''

    # These are valid values for a name query
    name = None
    vname = None
    fqname = None

    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''
        
        default = PartialMixin.ANY

        return ([ (k,default, True) 
                for k,_, _ in PartitionName._name_parts ]
                + 
               [('name',default,True),
                ('vname',default,True),
                ('fqname',default,True)]
               )

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
    assignment_class = 'self'
    
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
    
    # Number of digits in each assignment class
    DLEN.DATASET = (3,5,7,9)
    DLEN.DATASET_CLASSES=dict(authoritative=DLEN.DATASET[0], # Datasets registered by number authority . 
                              registered=DLEN.DATASET[1], # For registered users of a numbering authority
                              unregistered=DLEN.DATASET[2], # For unregistered users of a numebring authority
                              self=DLEN.DATASET[3]) # Self registered
    DLEN.PARTITION = 3
    DLEN.TABLE = 2
    DLEN.COLUMN = 3
    DLEN.REVISION = (0,3)
    
    # Because the dataset number can be 3, 5, 7 or 9 characters, 
    # And the revision is optional, the datasets ( and thus all 
    # other objects ) , can have several differnt lengths. We
    # Use these different lengths to determine what kinds of
    # fields to parse 
    # 's'-> short dataset, 'l'->long datset, 'r' -> has revision
    #
    # generate with:
    #     {
    #         ds_len+rl:(ds_len, (rl if rl != 0 else None), cls)
    #         for cls, ds_len in self.DLEN.ATASET_CLASSES.items()
    #         for rl in self.DLEN.REVISION
    #     }
    #     
    DATASET_LENGTHS = {
                        3: (3, None, 'authoritative'),
                        5: (5, None, 'registered'),
                        6: (3, 3, 'authoritative'),
                        7: (7, None, 'unregistered'),
                        8: (5, 3, 'registered'),
                        9: (9, None, 'self'),
                        10: (7, 3, 'unregistered'),
                        12: (9, 3, 'self')}

    # Length of the caracters that aren't the dataset and revisions
    NDS_LENGTH = {'d': 0,
                  'p': DLEN.PARTITION,
                  't': DLEN.TABLE,
                  'c': DLEN.TABLE+DLEN.COLUMN}
   
    TCMAXVAL = 62**DLEN.TABLE -1; # maximum for table values. 
    CCMAXVAL = 62**DLEN.COLUMN -1; # maximum for column values. 
    PARTMAXVAL = 62**DLEN.PARTITION -1; # maximum for table and column values. 
     
    EPOCH = 1389210331 # About Jan 8, 2014

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
        
        assignment_class = ds_lengths[2]
        
        dataset = int(ObjectNumber.base62_decode(input[0:ds_lengths[0]]))
        
        if ds_lengths[1]: 
            i = len(input)-ds_lengths[1]
            revision = int(ObjectNumber.base62_decode(input[i:]))
            input = input[0:i] # remove the revision
        else:
            revision = None
            
        input = input[ds_lengths[0]:]
      
        if type_ == cls.TYPE.DATASET:
            return DatasetNumber(dataset, revision=revision, assignment_class=assignment_class)
        
        elif type_ == cls.TYPE.TABLE:   
            table = int(ObjectNumber.base62_decode(input))
            return TableNumber(DatasetNumber(dataset, assignment_class=assignment_class), 
                               table, revision=revision)
        
        elif type_ == cls.TYPE.PARTITION:
            partition = int(ObjectNumber.base62_decode(input))
            return PartitionNumber(DatasetNumber(dataset, assignment_class=assignment_class), 
                                   partition, revision=revision)   
                   
        elif type_ == cls.TYPE.COLUMN:     
            table = int(ObjectNumber.base62_decode(input[0:cls.DLEN.TABLE]))
            column = int(ObjectNumber.base62_decode(input[cls.DLEN.TABLE:]))

            return ColumnNumber(TableNumber(
                                DatasetNumber(dataset, assignment_class=assignment_class), table), 
                                column, revision=revision)
        
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
                if bool(revision) else '')

class DatasetNumber(ObjectNumber):
    '''An identifier for a dataset'''
    def __init__(self, dataset=None, revision=None, assignment_class='self'):
        '''
        Constructor
        '''

        self.assignment_class = assignment_class

        if dataset is None:

            import random
            digit_length = self.DLEN.DATASET_CLASSES[self.assignment_class]
            # On 64 bit machine, max is about 10^17, 2^53
            # That should be random enough to prevent 
            # collisions for a small number of self assigned numbers
            max = 62**digit_length
            dataset = random.randint(0,max)
          
        
        self.dataset = dataset
        self.revision = revision
 
    def _ds_str(self):
        
        ds_len = self.DLEN.DATASET_CLASSES[self.assignment_class]
        
        return (ObjectNumber.base62_encode(self.dataset).rjust(ds_len,'0') )

    def __str__(self):        
        return (ObjectNumber.TYPE.DATASET+
                self._ds_str()+
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
                self.dataset._ds_str()+
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
                self.dataset._ds_str()+
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
        
        partition = int(partition)
        
        if not isinstance(dataset, DatasetNumber):
            raise ValueError("Constructor requires a DatasetNumber")

        if partition > ObjectNumber.PARTMAXVAL:
            raise ValueError("Value is too large. Max is: {}".format(ObjectNumber.PARTMAXVAL))

        self.dataset = dataset
        self.partition = partition
        self.revision = revision

        if not self.revision and dataset.revision:
            self.revision = dataset.revision
        
    def __str__(self):        
        return (ObjectNumber.TYPE.PARTITION+
                self.dataset._ds_str()+
                ObjectNumber.base62_encode(self.partition).rjust(self.DLEN.PARTITION,'0')+
                ObjectNumber._rev_str(self.revision))


   
class Identity(object):
    '''Identities represent the defining set of information about a 
    bundle or a partition. Only the vid is actually required to 
    uniquely identify a bundle or partition, but the identity is also
    used for generating unique names and for finding bundles and partitions. '''
    
    
    is_bundle = True
    is_partition = False

    _name_class = Name

    _on = None
    _name = None
    creator = None

    def __init__(self, name, object_number, creator=None):

        self._on = object_number
        self._name = name
        self.creator = creator

        if not self._name.type_is_compatible(self._on):
            raise TypeError("The name and the object number must be "+
                            "of compatible types: got {} and {}"
                            .format(type(name), type(object_number)))

        # Update the patch number to always be the revision
        nv = Version(self._name.version)
        nv.patch = self._on.revision
        self._name.version = str(nv)

    @classmethod
    def from_dict(cls, d):
        name = cls._name_class(**d)

        if 'id' in d and 'revision' in d:
            # The vid should be constructed from the id and the revision
            on = (ObjectNumber.parse(d['id']).rev(d['revision']))
        elif 'vid' in d:
            on = ObjectNumber.parse(d['vid'])
        else:
            raise ValueError("Must have id and revision, or vid")
          
        return cls(name, on, d.get('creator'))

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

    def is_valid(self):
        self._name.is_valid()

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
        """The name object"""
        return self._name

    @property
    def sname(self):
        """The name of the bundle, as a string, excluding the revision"""
        return str(self._name)


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
 
    @property
    def dict(self):
        d = self._name.dict
        
        d['vid'] = str(self._on)
        d['id'] = str(self._on.rev(None))
        d['revision'] = int(self._on.revision)
        d['creator'] = self.creator
        
        return d
 
    @staticmethod
    def _compose_fqname(vname, vid):
        return vname+'~'+vid
 
    def __str__(self):
        return self._compose_fqname(self._name.vname,self.vid)
       
    
       

class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''
    
    _name_class = PartitionName

    @property
    def table(self):
        return self._name.table

    @property
    def as_dataset(self):
        """Convert this identity to the identity of the corresponding dataset. """
        
        on = ObjectNumber.parse(self.id_)
        d = self.to_dict()
        d['id'] = str(on.dataset)
        
        return  Identity(**d)



class NumberServer(object):
    
    def __init__(self, host='numbers.ambry.io', port='80', key=None):
        
        self.host = host
        self.port = port
        self.key = key
        self.port_str = ':'+str(port) if port else ''

    def next(self):
        import requests
        
        if self.key:
            params = dict(access_key=self.key)
        else:
            params = dict()
        
        r = requests.get('http://{}{}/next'.format(self.host, self.port_str), params=params)
        r.raise_for_status()
        
        d = r.json()
        
        return d['number']


class Resolver(object):
    
    def resolve(self,s):
        pass
    
    
    def parse(self,s):
        
        name, id = s.split('--')
    
        # If the version
    

def make_resolver(library=None, bundle=None):
    '''Return a resolver object constructed on a library or bundle '''





