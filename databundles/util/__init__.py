"""Misc support code. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

# Stolen from: http://code.activestate.com/recipes/498245-lru-and-lfu-cache-decorators/
from __future__ import print_function
import collections
import functools
from itertools import ifilterfalse
from heapq import nsmallest
from operator import itemgetter
import logging
import yaml
from collections import Mapping, OrderedDict, defaultdict
import os 
import sys


logger_init = set()

## {{{ http://code.activestate.com/recipes/52549/ (r3)
class curry:
    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs

        return self.fun(*(self.pending + args), **kw)
## end of http://code.activestate.com/recipes/52549/ }}}




def get_logger(name, file_name = None):
    
    logger = logging.getLogger(name)

    if  name not in logger_init:

        formatter = logging.Formatter("%(name)s %(levelname)s %(message)s")
        
        if file_name:
            ch = logging.FileHandler(file_name)
        else:
            ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        #ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        #logger.setLevel(logging.DEBUG) 
        logger._stream = ch.stream
        logger_init.add(name)
     
    return logger


def rm_rf( d):
    """Recursively delete a directory"""
    
    if not os.path.exists(d):
        return
    
    for path in (os.path.join(d,f) for f in os.listdir(d)):
        if os.path.isdir(path):
            rm_rf(path)
        else:
            os.unlink(path)
    os.rmdir(d)

def bundle_file_type(path_or_file):
    '''Return a determination if the file is a sqlite file or a gzip file
    
    Args
        :param path: a string pathname to the file or stream to test
        :type path: a `str` object or file object
        
        :rtype: 'gzip' or 'sqlite' or None
    '''

    import struct

    try:
        try: loc = path_or_file.tell()
        except: loc = 0
        path_or_file.seek(0)
        d = path_or_file.read(15)
        path_or_file.seek(loc)
    except Exception as e:
        d = None
        
    if not d:
        try:
            with open(path_or_file) as f:
                d = f.read(15)
        except:
            d = None
            
            
    if not d:
        if path_or_file.endswith('.db'):
            return 'sqlite'
        elif path_or_file.endswith('.gz'):
            return 'gzip'
        else:
            Exception("Can't figure out file type for {}".format(path_or_file))
    
    d = d.strip()
    
    if d == 'SQLite format 3':
        return 'sqlite'
    elif d[1:4] == 'HDF':
        return 'hdf'
    elif hex(struct.unpack('!H',d[0:2])[0]) == '0x1f8b':
        return 'gzip'
    else:
        return None
        

        
class Counter(dict):
    'Mapping where default values are zero'
    def __missing__(self, key):
        return 0

def lru_cache(maxsize=128):
    '''Least-recently-used cache decorator.

    Arguments to the cached function must be hashable.
    Cache performance statistics stored in f.hits and f.misses.
    Clear the cache with f.clear().
    http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used

    '''
    maxqueue = maxsize * 10
    def decorating_function(user_function,
            len=len, iter=iter, tuple=tuple, sorted=sorted, KeyError=KeyError): #@ReservedAssignment
        cache = {}                  # mapping of args to results
        queue = collections.deque() # order that keys have been used
        refcount = Counter()        # times each key is in the queue
        sentinel = object()         # marker for looping around the queue
        kwd_mark = object()         # separate positional and keyword args

        # lookup optimizations (ugly but fast)
        queue_append, queue_popleft = queue.append, queue.popleft
        queue_appendleft, queue_pop = queue.appendleft, queue.pop

        @functools.wraps(user_function)
        def wrapper(*args, **kwds):
            # cache key records both positional and keyword args
            key = args
            if kwds:
                key += (kwd_mark,) + tuple(sorted(kwds.items()))

            # record recent use of this key
            queue_append(key)
            refcount[key] += 1

            # get cache entry or compute if not found
            try:
                result = cache[key]
                wrapper.hits += 1
            except KeyError:
                result = user_function(*args, **kwds)
                cache[key] = result
                wrapper.misses += 1

                # purge least recently used cache entry
                if len(cache) > maxsize:
                    key = queue_popleft()
                    refcount[key] -= 1
                    while refcount[key]:
                        key = queue_popleft()
                        refcount[key] -= 1
                    del cache[key], refcount[key]

            # periodically compact the queue by eliminating duplicate keys
            # while preserving order of most recent access
            if len(queue) > maxqueue:
                refcount.clear()
                queue_appendleft(sentinel)
                for key in ifilterfalse(refcount.__contains__,
                                        iter(queue_pop, sentinel)):
                    queue_appendleft(key)
                    refcount[key] = 1


            return result

        def clear():
            cache.clear()
            queue.clear()
            refcount.clear()
            wrapper.hits = wrapper.misses = 0

        wrapper.hits = wrapper.misses = 0
        wrapper.clear = clear
        return wrapper
    return decorating_function


def lfu_cache(maxsize=100):
    '''Least-frequenty-used cache decorator.

    Arguments to the cached function must be hashable.
    Cache performance statistics stored in f.hits and f.misses.
    Clear the cache with f.clear().
    http://en.wikipedia.org/wiki/Least_Frequently_Used

    '''
    def decorating_function(user_function):
        cache = {}                      # mapping of args to results
        use_count = Counter()           # times each key has been accessed
        kwd_mark = object()             # separate positional and keyword args

        @functools.wraps(user_function)
        def wrapper(*args, **kwds):
            key = args
            if kwds:
                key += (kwd_mark,) + tuple(sorted(kwds.items()))
            use_count[key] += 1

            # get cache entry or compute if not found
            try:
                result = cache[key]
                wrapper.hits += 1
            except KeyError:
                result = user_function(*args, **kwds)
                cache[key] = result
                wrapper.misses += 1

                # purge least frequently used cache entry
                if len(cache) > maxsize:
                    for key, _ in nsmallest(maxsize // 10,
                                            use_count.iteritems(),
                                            key=itemgetter(1)):
                        del cache[key], use_count[key]

            return result

        def clear():
            cache.clear()
            use_count.clear()
            wrapper.hits = wrapper.misses = 0

        wrapper.hits = wrapper.misses = 0
        wrapper.clear = clear
        return wrapper
    return decorating_function

def patch_file_open():
    ''' A Monkey patch to log opening and closing of files, which is useful for debugging
    file descriptor exhaustion'''
    import __builtin__
    openfiles = set()
    oldfile = __builtin__.file
    class newfile(oldfile):
        def __init__(self, *args,**kwargs):
            self.x = args[0]
            print ("### {} OPENING {} ###".format(len(openfiles), str(self.x)))        
            oldfile.__init__(self, *args,**kwargs)
            openfiles.add(self)
    
        def close(self):
            print ("### {} CLOSING {} ###".format(len(openfiles), str(self.x)))
            oldfile.close(self)
            openfiles.remove(self)
            
    def newopen(*args,**kwargs):
        return newfile(*args,**kwargs)
    
    __builtin__.file = newfile
    __builtin__.open = newopen

#patch_file_open()

# From http://stackoverflow.com/questions/528281/how-can-i-include-an-yaml-file-inside-another
class YamlIncludeLoader(yaml.Loader):

    def __init__(self, stream):

        self._root = os.path.split(stream.name)[0]

        super(YamlIncludeLoader, self).__init__(stream)


# From http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class OrderedDictYAMLLoader(yaml.Loader):
    'Based on: https://gist.github.com/844388'

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)
        
        self.dir = None
        for a in args:
            try:
                self.dir = os.path.dirname(a.name)
            except: pass

        
        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)
        self.add_constructor('!include', OrderedDictYAMLLoader.include)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError( None, None,
                'expected a mapping node, but found {}'.format(node.id), node.start_mark )

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError( 'while constructing a mapping',
                    node.start_mark, 'found unacceptable key ({})'.format(exc), key_node.start_mark )
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


    def include(self, node):
        from databundles.dbexceptions import ConfigurationError
        

        if not self.dir:   
            return "ConfigurationError: Can't include file: wasn't able to set base directory"

        relpath = self.construct_scalar(node)
        abspath = os.path.join(self.dir,relpath)

        if not os.path.exists(abspath):
            raise ConfigurationError("Can't include file '{}': Does not exist".format(abspath))

        with open(abspath, 'r') as f:
            
            parts = abspath.split('.')
            ext = parts.pop()
            
            if ext == 'yaml':
                return yaml.load(f, OrderedDictYAMLLoader)
            else:
                return IncludeFile(abspath, relpath, f.read())

# IncludeFile and include_representer ensures that when config files are re-written, they are
# represented as an include, not the contents of the include
class IncludeFile(str):
    
    def __new__(cls, abspath, relpath, data):
        s =  str.__new__(cls,  data)
        s.abspath = abspath
        s.relpath = relpath
        return s
        
    def __str__(self):
        return self.data
 
def include_representer(dumper, data):
    return dumper.represent_scalar(u'!include', data.relpath)
    

# http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class AttrDict(OrderedDict):

    def __init__(self, *argz, **kwz):
        super(AttrDict, self).__init__(*argz, **kwz)

    def __setitem__(self, k, v):
        super(AttrDict, self).__setitem__( k,
            AttrDict(v) if isinstance(v, Mapping) else v )
    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')): 
            return self[k]
        else: 
            return super(AttrDict, self).__getattr__(k)
    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)
        self[k] = v

    @classmethod
    def from_yaml(cls, path, if_exists=False):
        if if_exists and not os.path.exists(path): return cls()
        return cls(yaml.load(open(path), OrderedDictYAMLLoader))

    @staticmethod
    def flatten_dict(data, path=tuple()):
        dst = list()
        for k,v in data.iteritems():
            k = path + (k,)
            if isinstance(v, Mapping):
                for v in v.flatten(k): dst.append(v)
            else: dst.append((k, v))
        return dst

    def flatten(self, path=tuple()):
        return self.flatten_dict(self, path=path)


    def to_dict(self):
        root  = {}
        val = self.flatten()
        for k,v in val:
            dst = root
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = dict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping ): 
                dst[k[-1]] = v

        return  root

    def update_flat(self, val):
        if isinstance(val, AttrDict): val = val.flatten()
        for k,v in val:
            dst = self
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = AttrDict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping ): 
                dst[k[-1]] = v

    def update_yaml(self, path): 
        self.update_flat(self.from_yaml(path))

    def clone(self):
        clone = AttrDict()
        clone.update_dict(self)
        return clone

    def rebase(self, base):
        base = base.clone()
        base.update_dict(self)
        self.clear()
        self.update_dict(base)

    def dump(self, stream):
        yaml.representer.SafeRepresenter.add_representer(
            AttrDict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            OrderedDict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            defaultdict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            set, yaml.representer.SafeRepresenter.represent_list )
        
        yaml.representer.SafeRepresenter.add_representer(
            IncludeFile, include_representer)
        
        yaml.safe_dump( self, stream,
            default_flow_style=False, indent=4, encoding='utf-8' )

from collections import Mapping

class CaseInsensitiveDict(Mapping): #http://stackoverflow.com/a/16202162
    def __init__(self, d):
        self._d = d
        self._s = dict((k.lower(), k) for k in d)
        
    def __contains__(self, k):
        return k.lower() in self._s
    def __len__(self):
        return len(self._s)
    def __iter__(self): 
        return iter(self._s)
    def __getitem__(self, k):
        return self._d[self._s[k.lower()]]
    def __setitem__(self, k, v):
        self._d[k] = v
        self._s[k.lower()] = k
    def pop(self, k):
        k0 = self._s.pop(k.lower())
        return self._d.pop(k0)
    def actual_key_case(self, k):
        return self._s.get(k.lower())

def lowercase_dict(d):
    return dict((k.lower(), v) for k,v in d.items())

def configure_logging(cfg, custom_level=None):
    '''Don't know what this is for .... '''
    import itertools as it, operator as op

    if custom_level is None: custom_level = logging.WARNING
    for entity in it.chain.from_iterable(it.imap(
            op.methodcaller('viewvalues'),
            [cfg] + list(cfg.get(k, dict()) for k in ['handlers', 'loggers']) )):
        if isinstance(entity, Mapping)\
            and entity.get('level') == 'custom': entity['level'] = custom_level
    logging.config.dictConfig(cfg)
    logging.captureWarnings(cfg.warnings)

## {{{ http://code.activestate.com/recipes/578272/ (r1)
def toposort(data):
    """Dependencies are expressed as a dictionary whose keys are items
and whose values are a set of dependent items. Output is a list of
sets in topological order. The first set consists of items with no
dependences, each subsequent set consists of items that depend upon
items in the preceeding sets.

>>> print '\\n'.join(repr(sorted(x)) for x in toposort2({
...     2: set([11]),
...     9: set([11,8]),
...     10: set([11,3]),
...     11: set([7,5]),
...     8: set([7,3]),
...     }) )
[3, 5, 7]
[8, 11]
[2, 9, 10]

"""

    from functools import reduce

    # Ignore self dependencies.
    for k, v in data.items():
        v.discard(k)
    # Find all items that don't depend on anything.
    extra_items_in_deps = reduce(set.union, data.itervalues()) - set(data.iterkeys())
    # Add empty dependences where needed
    data.update({item:set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item, dep in data.iteritems() if not dep)
        if not ordered:
            break
        yield ordered
        data = {item: (dep - ordered)
                for item, dep in data.iteritems()
                    if item not in ordered}
        
    assert not data, "Cyclic dependencies exist among these items:\n%s" % '\n'.join(repr(x) for x in data.iteritems())
## end of http://code.activestate.com/recipes/578272/ }}}

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
        
        
def zip_dir(dir, file_):
    
    import zipfile, glob
    with zipfile.ZipFile(file_, 'w') as zf:
        g = os.path.join(dir,'*')
        for f in glob.glob(g):
            zf.write(f)
    return dir
    
    
def md5_for_file(f, block_size=2**20):
    """Generate an MD5 has for a possibly large file by breaking it into chunks"""
    import hashlib
    
    md5 = hashlib.md5()
    try:
        # Guess that f is a FLO. 
        f.seek(0)
        
        while True:
            data = f.read(block_size)
            if not data:
                break 
            md5.update(data)
        return md5.hexdigest()
    
    except AttributeError as e: 
        # Nope, not a FLO. Maybe string?

        file_name = f
        with open(file_name, 'rb') as f:
            return md5_for_file(f, block_size)

  

def rd(v, n=100.0):
    """Round down, to the nearest even 100"""
    import math
    n = float(n)
    return math.floor(v/n) * int(n)

def ru(v, n = 100.0):
    """Round up, to the nearest even 100"""
    import math
    n = float(n)
    return math.ceil(v/n) * int(n)


def make_acro(past, prefix, s):
    """Create a three letter acronym from the input string s
    
    Args:
        past: A set object, for storing acronyms that have already been created
        prefix: A prefix added to the acronym before storing in the set
        s: The string to create the acronym from. 
    
    
    """
    
    def _make_acro( s, t=0):
        import re
        # Really should cache these ... 
        v = ['a','e','i','o','u','y']
        c = [ chr(x) for x in range(ord('a'), ord('z')+1) if chr(x) not in v]
        
        s = re.sub(r'\W+', '',s.lower())
        
        vx = [ x for x in s if x in v ]
        cx = [ x for x in s if x in c ]

        if s.startswith('Mc'):
            
            if t < 1: return 'Mc' + v[0]
            if t < 2: return 'Mc' + c[0]

        if s[0] in v: # Starts with a vowel
            if t < 1:  return vx[0]+cx[0]+cx[1]
            if t < 2:  return vx[0]+vx[1]+cx[0]
        
        if s[0] in c and s[1] in c: # Two first consonants
            if t < 1:  return cx[0]+cx[1]+vx[0]
            if t < 2:  return cx[0]+cx[1]+cx[2]
        
       
        if t < 3: return cx[0]+vx[0]+cx[1]
        if t < 4: return cx[0]+cx[1]+cx[2]
        if t < 5: return cx[0]+vx[0]+vx[1]
        if t < 6: return cx[0]+cx[1]+cx[-1]
        if t < 7: return s[0:3]
        if t < 8: return s[1:4]
        if t < 9: return s[2:5]
        if t < 10: return s[3:6]
        
        return None

 
    
    for t in [0,1,2,3,4,5,6,7,8,9,10]:
        
        try:
            a = _make_acro(s,t)
            
            if a is not None:
                if prefix:
                    aps = prefix+a
                else:
                    aps = a
                    
                if aps not in past:
                    past.add(aps)
                    return a
            
        except IndexError:
            pass
        
    raise Exception("Could not get acronym")

def temp_file_name():
    '''Create a path to a file in the temp directory'''
    
    import tempfile
    
    f = tempfile.NamedTemporaryFile(delete=False)
    f.close()
    
    return f.name
    
# http://stackoverflow.com/questions/296499/how-do-i-zip-the-contents-of-a-folder-using-python-version-2-5
def zipdir(basedir, archivename):
    from contextlib import closing
    from zipfile import ZipFile, ZIP_DEFLATED
    import os

    assert os.path.isdir(basedir)
    
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(basedir):
           
            #NOTE: ignore empty directories
            for fn in files:
           
                absfn = os.path.join(root, fn)
                zfn = absfn[len(basedir)+len(os.sep):] #XXX: relative path
                z.write(absfn, zfn)
  
# from https://github.com/kennethreitz/requests/issues/465  
class FileLikeFromIter(object):
    def __init__(self, content_iter, cb=None, buffer_size = 128*1024):

        self._iter = content_iter
        self.data = ''
        self.time = 0
        self.prt = 0
        self.cum = 0
        self.cb = cb
        self.buffer_size = buffer_size
        self.buffer = memoryview(bytearray('\0'*buffer_size))
        self.buffer_alt = memoryview(bytearray('\0'*buffer_size))

    def __iter__(self):
        return self._iter

    def x_read(self,n=None):
        
        if n is None:
            raise Exception("Can't read from this object without a length")
        
        while self.prt < n:
            try:
                d = self._iter.next()
                l = len(d)
                self.buffer[self.prt:(self.prt+l)] = d
                self.prt += l
            except StopIteration:
                break
        
        if self.prt < n:
            # Done!
            d = self.buffer[:self.prt].tobytes()
            self.buffer_alt = memoryview(bytearray('\0'*self.buffer_size))
            self.buffer = memoryview(bytearray('\0'*self.buffer_size))
            self.prt = 0
            return d
        else:  
            # Save the excess in the alternate buffer, miving it to the
            # start so we can append to it next call. 
            self.buffer_alt[0:self.prt - n] = self.buffer[n:self.prt]
           
            #Swap the buffers, so we start by appending to the excess on the next read       
            self.buffer, self.buffer_alt = self.buffer_alt, self.buffer
    
            self.prt = self.prt - n
    
            if self.cb:
                self.cum += n
                self.cb(self.cum)
    
            return self.buffer_alt[0:n].tobytes()
    

    def read(self, n=None):

        if n is None:
            return self.data + ''.join(l for l in self._iter)
        else:
            while len(self.data) < n:
                try:
                    self.data = ''.join((self.data, self._iter.next()))
                except StopIteration:
                    break
            
            result, self.data = self.data[:n], self.data[n:]

            self.cum += n
            
            if self.cb:
                self.cb(self.cum)

            return result

    

def walk_dict(d):
    '''
    Walk a tree (nested dicts).

    For each 'path', or dict, in the tree, returns a 3-tuple containing:
    (path, sub-dicts, values)

    where:
    * path is the path to the dict
    * sub-dicts is a tuple of (key,dict) pairs for each sub-dict in this dict
    * values is a tuple of (key,value) pairs for each (non-dict) item in this dict
    '''
    # nested dict keys
    nested_keys = tuple(k for k in d.keys() if isinstance(d[k],dict))
    # key/value pairs for non-dicts
    items = tuple((k,d[k]) for k in d.keys() if k not in nested_keys)

    # return path, key/sub-dict pairs, and key/value pairs
    yield ('/', [(k,d[k]) for k in nested_keys], items)

    # recurse each subdict
    for k in nested_keys:
        for res in walk_dict(d[k]):
            # for each result, stick key in path and pass on
            res = ('/%s' % k + res[0], res[1], res[2])
            yield res


def copy_file_or_flo(input_, output, buffer_size=64*1024, cb=None):
    """ Copy a file name or file-like-object to another
    file name or file-like object"""
    import shutil 
    
    input_opened = False
    output_opened = False
    try:
        if isinstance(input_, basestring):
            
            if not os.path.isdir(os.path.dirname(input_)):
                os.makedirs(os.path.dirname(input_))
            
            input_ = open(input_,'r')
            input_opened = True
    
        if isinstance(output, basestring):
            
            if not os.path.isdir(os.path.dirname(output)):
                os.makedirs(os.path.dirname(output))
            
            output = open(output,'wb')   
            output_opened = True 
            
        #shutil.copyfileobj(input_,  output, buffer_size)
        
        def copyfileobj(fsrc, fdst, length=16*1024):
            cumulative = 0
            while 1:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if cb:
                    cumulative += len(buf)
                    cb(cumulative)
        
        copyfileobj(input_, output)
        
    finally:
        if input_opened:
            input_.close()
            
        if output_opened:
            output.close()


def _log_rate(d, message=None, prt=None):
    """Log a message for the Nth time the method is called.
    
    d is the object returned from init_log_rate
    """
    
    import time 

    if not prt:
        prt = print 

    if not d[1]:
        d[1] = time.clock()

    if not message:
        message = d[3]

    d[0] += 1
    print (d[0],d[2], d[0] % d[2])
    if  d[0] % d[2] == 0:
        # Prints the processing rate in 1,000 records per sec.
        rate = int( d[0]/(time.clock()-d[1]))
        prt(message+': '+str(rate)+'/s '+str(d[0]/1000)+"K ") 
    

        
def init_log_rate(N, message=''):
    """Initialze the log_rate function. Returnas a partial function to call for
    each event"""
  
    import functools 
    
    d =  [0,  # number of items processed
            None, # start time
            N,  #frequency to log a message
            message]

    f = functools.partial(_log_rate, d)
    f.always = print
    return f

def daemonize(f, args,  rc, prog_name='databundles'):
        '''Run a process as a daemon'''
        import daemon #@UnresolvedImport
        import lockfile  #@UnresolvedImport
        import setproctitle #@UnresolvedImport
        import os, sys
        import grp, pwd
        import logging
            
        if args.kill:
            # Not portable, but works in most of our environments. 
            import os
            print("Killing ... ")
            os.system("pkill -f '{}'".format(prog_name))
            return
        
        lib_dir = '/var/lib/'+prog_name
        run_dir = '/var/run/'+prog_name
        log_dir = '/var/log/'+prog_name
        log_file = os.path.join(log_dir,prog_name+'.stdout')
        
        logger = get_logger(prog_name, log_file)
        logger.setLevel(logging.DEBUG) 
        
        lock_file_path = os.path.join(run_dir,prog_name+'.pid')
        pid_file = lockfile.FileLock(lock_file_path)
        
        if pid_file.is_locked():
            if args.unlock:
                pid_file.break_lock()
            else:
                logger.error("Lockfile is locked: {}".format(lock_file_path))
                sys.stderr.write("ERROR: Lockfile is locked: {}\n".format(lock_file_path))
                sys.exit(1)

        for dir in [run_dir, lib_dir, log_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir)

        gid =  grp.getgrnam(args.group).gr_gid if args.group is not None else os.getgid()
        uid =  pwd.getpwnam(args.user).pw_uid if args.user  is not None else os.getuid()  

        class DaemonContext(daemon.DaemonContext):
     
            def __exit__(self,exc_type, exc_value, exc_traceback):
                
                logger.info("Exiting")

                super(DaemonContext, self).__exit__(exc_type, exc_value, exc_traceback)


        context = DaemonContext(
            detach_process = False,
            working_directory=lib_dir,
            umask=0o002,
            pidfile=pid_file,
            gid  = gid, 
            uid = uid,
            files_preserve = [logger._stream]
            )
        
      
        os.chown(log_file, uid, gid);
        os.chown(lib_dir, uid, gid);
        os.chown(run_dir, uid, gid);
        os.chown(log_dir, uid, gid);

        setproctitle.setproctitle(prog_name)
                
        with context:
            f(prog_name, args, rc, logger)
            

# from http://stackoverflow.com/questions/6796492/python-temporarily-redirect-stdout-stderr
# Use as a context manager
class RedirectStdStreams(object):
    def __init__(self, stdout=None, stderr=None):
        
        self.devnull = open(os.devnull, 'w')
        
        self._stdout = stdout or self.devnull
        self._stderr = stderr or self.devnull

    def __enter__(self):
        
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.devnull.close()

def _print(*args):
    print(*args)

class Progressor(object):
    '''Progress reporter suitable for calling in Library.get()
    
    Example:  r = l.get(args.term, cb=Progressor().progress)
    
    '''

    start = None
    last = None
    freq = 1

    def __init__(self, message='Download', printf = _print):
        import time
        from collections import deque
        self.start = time.clock()
        self.message = message
        self.rates = deque(maxlen=10)
        self.printf = printf
        

    def progress(self, i, n):
        import curses
        import time
        
        import time
        now = time.clock()

        if not self.last:
            self.last = now
        
        if now - self.last > self.freq:
            diff = now - self.start 
            i_rate = float(i)/diff
            self.rates.append(i_rate)
            
            if len(self.rates) > self.rates.maxlen/2:
                rate = sum(self.rates) / len(self.rates)
                rate_type = 'a'
            else:
                rate = i_rate
                rate_type = 'i'

            self.printf("{}: Compressed: {} Mb. Downloaded, Uncompressed: {:6.2f}  Mb, {:5.2f} Mb / s ({})".format(
                 self.message,int(int(n)/(1024*1024)),
                 round(float(i)/(1024.*1024.),2), 
                 round(float(rate)/(1024*1024),2), rate_type))
            
            self.last = now
            
      
