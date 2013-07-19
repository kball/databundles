"""Access objects for the file system within a bundle and for filesystem caches
used by the download processes and the library. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os
import io

from databundles.orm import File
import zipfile
import gzip
import urllib
import databundles.util
import logging
from databundles.identity import Identity

logger = databundles.util.get_logger(__name__)
#logger.setLevel(logging.DEBUG) 
        
##makedirs
## Monkey Patch!
## Need to patch zipfile.testzip b/c it doesn't close file descriptors in 2.7.3
## The bug apparently exists in several other versions of python 
## http://bugs.python.org/issue16408
def  testzip(self):
    """Read all the files and check the CRC."""
    chunk_size = 2 ** 20
    for zinfo in self.filelist:
        try:
            # Read by chunks, to avoid an OverflowError or a
            # MemoryError with very large embedded files.
            f = self.open(zinfo.filename, "r")
            while f.read(chunk_size):     # Check CRC-32
                pass
            f.close()
            f._fileobj.close() # This shoulnd't be necessary, but it is. 
        except zipfile.BadZipfile:
            return zinfo.filename

zipfile.ZipFile.testzip = testzip

class DownloadFailedError(Exception):
    pass

class FileRef(File):
    '''Extends the File orm class with awareness of the filsystem'''
    def __init__(self, bundle):
        
        self.super_ = super(FileRef, self)
        self.super_.__init__()
        
        self.bundle = bundle
        
    @property
    def abs_path(self):
        return self.bundle.filesystem.path(self.path)
    
    @property
    def changed(self):
        return os.path.getmtime(self.abs_path) > self.modified
       
    def update(self):
        self.modified = os.path.getmtime(self.abs_path)
        self.content_hash = Filesystem.file_hash(self.abs_path)
        self.bundle.database.session.commit()

class Filesystem(object):
    
    def __init__(self, config):
        self.config = config
     
    def get_cache(self, cache_name, config=None):

        """Return a new :class:`FsCache` built on the configured cache directory
  
        :type cache_name: string
        :param cache_name: A key in the 'filesystem' section of the configuration,
            from which configuration will be retrieved
     
        :type config: :class:`RunConfig`
        :param config: If supplied, wil replace the default RunConfig()
              
        :rtype: a `FsCache` object
        :return: a nre first level cache. 
        
        If config is None, the function will constuct a new RunConfig() with a default
        constructor. 
        
        The `FsCache` will be constructed with the cache_dir values from the
        library.cache config key, and if the library.repository value exists, it will 
        be use for the upstream parameter.
    
        """   

        if config is None:
            config = self.config

        return Filesystem._get_cache(config.filesystem, cache_name)

    @classmethod
    def _get_cache(cls, filesystem_config, cache_name, subconfig=None):
        import tempfile
        from databundles.dbexceptions import ConfigurationError
        
        if not filesystem_config:
            raise ConfigurationError("Didn't get filesystem configuration value. from config files: ")

        if subconfig is None:
            subconfig = filesystem_config.get(cache_name)
        
        if subconfig is None:
            raise ConfigurationError("Didn't get cache name '{}' from config. keys: {}".format(cache_name, filesystem_config.keys()))
        
        root_dir = filesystem_config.get('root_dir',tempfile.gettempdir())
        
        from databundles.dbexceptions import ConfigurationError
        
        cache = None
        if subconfig.get('dir',False):

            dir = subconfig.get('dir').format(root=root_dir)
            
            if subconfig.get('size', False):
                cache =  FsLimitedCache(dir, maxsize=subconfig.get('size',10000))
            else:
                cache =  FsCache(dir)               
                
        elif subconfig.get('bucket',False):
            
            cache =  S3Cache(bucket=subconfig.get('bucket'), 
                    prefix=subconfig.get('prefix', None),
                    access_key=subconfig.get('access_key'),
                    secret=subconfig.get('secret'))
        else:
            raise ConfigurationError("Can't determine type of cache for key: {}".format(cache_name))
        
        if subconfig.get('upstream',False):
            up_name = cache_name+'.upstream'

            upstream = subconfig.get('upstream')

            # If it is a string, its a name of another filesystem entry
            if isinstance(upstream, basestring):
                upstream_name = upstream
                upstream = filesystem_config.get(upstream_name,False)
                
                if not upstream:
                    raise ConfigurationError("Failed to get config for: {}".format(upstream_name))

            cache.upstream = cls._get_cache(filesystem_config, up_name, upstream)
        
        if subconfig.get('options',False) and isinstance(subconfig.get('options',False), list):
         
            # Must come before 'compress'!
            if 'remote' in subconfig.get('options'):
                cache.usreadonly = True
                cache.is_remote = True
         
            if 'compress' in subconfig.get('options'):
                cache = FsCompressionCache(cache)

            if 'readonly' in subconfig.get('options'):
                cache.readonly = True
                
            if 'usreadonly' in subconfig.get('options'):
                cache.usreadonly = True

                
                

        return cache
        
    @classmethod
    def find_f(cls, config, key, value):
        '''Find a filesystem entry where the key `key` equals `value`'''
        
    @classmethod
    def rm_rf(cls, d):
        
        if not os.path.exists(d):
            return
        
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)


class BundleFilesystem(Filesystem):
    
    BUILD_DIR = 'build'
    META_DIR = 'meta'
    
    def __init__(self, bundle, root_directory = None):
        
        super(BundleFilesystem, self).__init__(bundle.config._run_config)
        
        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_dir()
 
        if not os.path.exists(self.path(BundleFilesystem.BUILD_DIR)):
            os.makedirs(self.path(BundleFilesystem.BUILD_DIR),0755)
 
    @staticmethod
    def find_root_dir(testFile='bundle.yaml', start_dir =  None):
        '''Find the parent directory that contains the bundle.yaml file '''
        import sys

        if start_dir is not None:
            d = start_dir
        else:
            d = sys.path[0]
        
        while os.path.isdir(d) and d != '/':
        
            test =  os.path.normpath(d+'/'+testFile)

            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)
             
        return None
    
    @property
    def root_dir(self):
        '''Returns the root directory of the bundle '''
        return self.root_directory

    def ref(self,rel_path):
        
        s = self.bundle.database.session
        import sqlalchemy.orm.exc
        
        try:
            o = s.query(FileRef).filter(FileRef.path==rel_path).one()
            o.bundle = self.bundle
        
            return o
        except  sqlalchemy.orm.exc.NoResultFound as e:
            raise e

    def path(self, *args):
        '''Resolve a path that is relative to the bundle root into an 
        absoulte path'''
     
        if len(args) == 0:
            raise ValueError("must supply at least one argument")
        
        args = (self.root_directory,) +args
            
        try:
            p = os.path.normpath(os.path.join(*args))    
        except AttributeError as e:
            raise ValueError("Path arguments aren't valid when generating path:"+ e.message)
        dir_ = os.path.dirname(p)
        if not os.path.exists(dir_):
            try:
                os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
            except Exception as e: #@UnusedVariable
                pass
            
            if not os.path.exists(dir_):
                raise Exception("Couldn't create directory "+dir_)

        return p

    

    def build_path(self, *args):
    
        if len(args) > 0 and args[0] == self.BUILD_DIR:
            raise ValueError("Adding build to existing build path "+os.path.join(*args))
        
        args = (self.BUILD_DIR,) + args
        return self.path(*args)


    def meta_path(self, *args):
    
        if len(args) > 0 and args[0] == self.META_DIR:
            raise ValueError("Adding meta to existing meta path "+os.path.join(*args))
        
        args = (self.META_DIR,) + args
        return self.path(*args)
    
    def directory(self, rel_path):
        '''Resolve a path that is relative to the bundle root into 
        an absoulte path'''
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path) ):
            os.makedirs(abs_path)
        return abs_path
 
    @staticmethod
    def file_hash(path):
        '''Compute hash of a file in chunks'''
        import hashlib
        md5 = hashlib.md5()
        with open(path,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), b''): 
                md5.update(chunk)
        return md5.hexdigest()
 


    def _get_unzip_file(self, cache, tmpdir, zf, path, name):
        '''Look for a member of a zip file in the cache, and if it doesn next exist, 
        extract and cache it. '''
        name = name.replace('..','')
        
        if name[0] == '/':
            name = name[1:]
        
        base = os.path.basename(path)
        
        rel_path = (urllib.quote_plus(base.replace('/','_'),'_')+'/'+
                    urllib.quote_plus(name.replace('/','_'),'_') )
     
        # Check if it is already in the cache
        cached_file = cache.get(rel_path)
        
        if cached_file:
            return cached_file
     
        # Not in cache, extract it. 
        tmp_abs_path = os.path.join(tmpdir, name)
        
        if not os.path.exists(tmp_abs_path):
            zf.extract(name,tmpdir )
            
        # Store it in the cache.           
        abs_path = cache.put(tmp_abs_path, rel_path)
        
        # There have been zip files that have been truncated, but I don't know
        # why. this is a stab i the dark to catch it. 
        if self.file_hash(tmp_abs_path) != self.file_hash(abs_path):
            raise Exception('Zip file extract error: md5({}) != md5({})'
                            .format(tmp_abs_path,abs_path ))

        return abs_path
 
    def unzip(self,path, regex=None):
        '''Context manager to extract a single file from a zip archive, and delete
        it when finished'''
        import tempfile, uuid
        
        cache = self.get_cache('extracts')

        tmpdir = tempfile.mkdtemp(str(uuid.uuid4()))
   
        try:
            with zipfile.ZipFile(path) as zf:
                abs_path = None
                if regex is None:
                    name = iter(zf.namelist()).next() # Assume only one file in zip archive. 
                    abs_path = self._get_unzip_file(cache, tmpdir, zf, path, name)   
                else:
                    
                    for name in zf.namelist():
                        if regex.match(name):
                            abs_path = self._get_unzip_file(cache, tmpdir, zf, path, name)   
                            break 
                        

                return abs_path
        finally:
            self.rm_rf(tmpdir)
            
        return None

    def unzip_dir(self,path,   regex=None):
        '''Context manager to extract a single file from a zip archive, and delete
        it when finished'''
        import tempfile, uuid
        
        cache = self.get_cache('extracts')

        tmpdir = tempfile.mkdtemp(str(uuid.uuid4()))
   
        try:
            with zipfile.ZipFile(path) as zf:
                abs_path = None 
                for name in zf.namelist():
                    abs_path = self._get_unzip_file(cache, tmpdir, zf, path, name)  
                    if regex and regex.match(name) or not regex:
                        yield abs_path
        except:
            self.bundle.error("File '{}' can't be unzipped".format(path))
            raise
        finally:
            self.rm_rf(tmpdir)
            
        return
        
    def download(self,url, test_f=None):
        '''Context manager to download a file, return it for us, 
        and delete it when done.
        
        Will store the downloaded file into the cache defined
        by filesystem.download
        '''

        import tempfile
        import urlparse
        import urllib2
      
        cache = self.get_cache('downloads')
        parsed = urlparse.urlparse(url)
        file_path = parsed.netloc+'/'+urllib.quote_plus(parsed.path.replace('/','_'),'_')

        # We download to a temp file, then move it into place when 
        # done. This allows the code to detect and correct partial
        # downloads. 
        download_path = os.path.join(tempfile.gettempdir(),file_path+".download")
          
        def test_zip_file(f):
            if not os.path.exists(f):
                raise Exception("Test zip file does not exist: {} ".format(f))
            
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False
          
        if test_f == 'zip':
            test_f = test_zip_file
          
        for attempts in range(3):
   
            if attempts > 0:
                self.bundle.error("Retrying download of {}".format(url))

            cached_file = None
            out_file = None
            excpt = None
                        
            try:                  

                cached_file = cache.get(file_path)
   
                if cached_file:
                 
                    out_file = cached_file
                    
                    if test_f and not test_f(out_file):
                        cache.remove(file_path, True)
                        raise DownloadFailedError("Cached Download didn't pass test function "+url)
     
                else:


                    self.bundle.log("Downloading "+url)
                    self.bundle.log("  --> "+file_path)
                    
                    resp = urllib2.urlopen(url)
                    headers = resp.headers #@UnusedVariable
                    
                    if resp.code != 200:
                        raise DownloadFailedError("Failed to download {}: code: "+format(url, resp.code))
                    
                    try:
                        out_file = cache.put(resp, file_path)
                    except:
                        self.bundle.error("Caught exception, deleting download file")
                        cache.remove(file_path)
                        raise
              
                    if test_f and not test_f(out_file):
                        cache.remove(file_path)
                        raise DownloadFailedError("Download didn't pass test function "+url)

                break
                
            except KeyboardInterrupt:
                cache.remove(file_path)
                raise
            except DownloadFailedError as e:
                self.bundle.error("Failed:  "+str(e))
                excpt = e
            except IOError as e:
                self.bundle.error("Failed to download "+url+" to "+file_path+" : "+str(e))
                excpt = e
            except urllib.ContentTooShortError as e:
                self.bundle.error("Content too short for "+url)
                excpt = e
            except zipfile.BadZipfile as e:
                # Code that uses the yield value -- like th filesystem.unzip method
                # can throw exceptions that will propagate to here. Unexpected, but very useful. 
                # We should probably create a FileNotValueError, but I'm lazy. 
                self.bundle.error("Got an invalid zip file for "+url)
                cache.remove(file_path)
                excpt = e
                
            except Exception as e:
                self.bundle.error("Unexpected download error '"+str(e)+"' when downloading "+url)
                cache.remove(file_path)
                raise 
    

        if download_path and os.path.exists(download_path):
            os.remove(download_path) 

        if excpt:
            raise excpt

        return out_file

    def read_csv(self, f, key = None):
        """Read a CSV into a dictionary of dicts or list of dicts
        
        Args:
            f a string or file object ( a FLO with read() )
            key columm or columns to use as the key. If None, return a list
        
        """
        
        opened = False
        if isinstance(f, basestring):
            f = open(f,'rb')
            opened = True
        
        import csv
        
        reader  = csv.DictReader(f)
    
        if key is None:
            out = []
        else:
            if isinstance(key, (list,tuple)):
                def make_key(row):
                    return tuple([ str(row[i].strip()) if row[i].strip() else None for i in key])
            else:
                def make_key(row):
                    return row[key]
                
            out = {}
    
        for row in reader:
                     
            if key is None:
                out.append(row)
            else:
                out[make_key(row)] = row
        
        if opened:
            f.close



        return out

    def download_shapefile(self, url):
        """Downloads a shapefile, unzips it, and returns the .shp file path"""
        import os
        import re
                
        zip_file = self.download(url)
        
        if not zip_file or not os.path.exists(zip_file):
            raise Exception("Failed to download: {} ".format(url))
            
        for file_ in self.unzip_dir(zip_file, 
                                regex=re.compile('.*\.shp$')): pass # Should only be one
        
        if not file_ or not os.path.exists(file_):
            raise Exception("Failed to unzip {} and get .shp file ".format(zip_file))
        
        return file_
        
    def load_yaml(self,*args):
        """Load a yaml file from the bundle file system. Arguments are passed to self.path()
        And if the first path element is not absolute, pre-pends the bundle path. 
        
        Returns an AttrDict of the results. 
        """
        from databundles.util import AttrDict  
        
        f = self.path(*args)
        
        ad = AttrDict()
        ad.update_yaml(f)
        
        return ad



    def get_url(self,source_url, create=False):
        '''Return a database record for a file'''
    
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session
        
        try:
            o = (s.query(File).filter(File.source_url==source_url).one())
         
        except sqlalchemy.orm.exc.NoResultFound:
            if create:
                o = File(source_url=source_url,path=source_url,process='none' )
                s.add(o)
                s.commit()
            else:
                return None
          
          
        o.session = s # Files have SavableMixin
        return o
    
    def get_or_new_url(self, source_url):
        return self.get_url(source_url, True)
 
    def add_file(self, rel_path):
        return self.filerec(rel_path, True)
    
    def filerec(self, rel_path, create=False):
        '''Return a database record for a file'''
  
        import sqlalchemy.orm.exc

        s = self.bundle.database.session
        
        if not rel_path:
            raise ValueError('Must supply rel_path')
        
        try:
            o = (s.query(File).filter(File.path==rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound as e:
           
            if not create:
                raise e
           
            a_path = self.filesystem.path(rel_path)
            o = File(path=rel_path,
                     content_hash=Filesystem.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True
            
        except Exception as e:
            return None
        
        return o
   

class FsCache(object):
    '''A cache that transfers files to and from a remote filesystem
    
    The `FsCache` stores files in a filesystem, possily retrieving and storing
    files to an upstream cache. 
    
    When files are written , they are written through to the upstream. If a file
    is requested that does not exist, it is fetched from the upstream. 
    
    When a file is added that causes the disk usage to exceed `maxsize`, the oldest
    files are deleted to free up space. 
    '''

    def __init__(self, cache_dir,  upstream=None):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''
        self.readonly = False
        self.usreadonly = False
        from databundles.dbexceptions import ConfigurationError
        self.cache_dir = cache_dir
        self.upstream = upstream
     
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        if not os.path.isdir(self.cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self.cache_dir)) 
        
    @property
    def connection_info(self):
        '''Return reference to the connection, excluding the secret'''
        if self.upstream:
            return self.upstream.connection_info
        else:
            return {'service':'file','dir':self.cache_dir }
        
    @property
    def remote(self):
        '''Return a reference to an inner cache that is a remote'''
        if self.upstream:
            return self.upstream.remote
        else:
            return None
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.cache_dir)

        return m.hexdigest()
    
    def get_stream(self, rel_path):
        p = self.get(rel_path)
        
        if p:
            return open(p) 
     
        if not self.upstream:
            return None

        return self.upstream.get_stream(rel_path)
        
    def path(self, rel_path):
        abs_path = os.path.join(self.cache_dir, rel_path)
        
        if os.path.exists(abs_path):
            return abs_path
        
        if self.upstream:
            return self.upstream.path(rel_path)        
        
        return False
        
    def has(self, rel_path, md5=None):
        
        abs_path = os.path.join(self.cache_dir, rel_path)
        
        if os.path.exists(abs_path):
            return abs_path
        
        if self.upstream:
            return self.upstream.has(rel_path, md5=md5)
        
        return False
        
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        import shutil

        logger.debug("FC {} get looking for {}".format(self.repo_id,rel_path)) 
               
        path = os.path.join(self.cache_dir, rel_path)
      
        # If is already exists in the repo, just return it. 
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            logger.debug("FC {} get {} found ".format(self.repo_id, path))
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
     
        stream = self.upstream.get_stream(rel_path)
        
        if not stream:
            logger.debug("FC {} get not found in upstream ()".format(self.repo_id,rel_path)) 
            return None
        
        # Got a stream from upstream, so put the file in this cache. 
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        
        with open(path,'w') as f:
            shutil.copyfileobj(stream, f)
        
        stream.close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        logger.debug("FC {} got return from upstream {}".format(self.repo_id,rel_path)) 
        return path
    

    def put(self, source, rel_path, metadata=None):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''

        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key

        sink = self.put_stream(rel_path, metadata=metadata)
        
        copy_file_or_flo(source, sink)
        
        sink.close()

        return sink.repo_path

    def put_stream(self,rel_path, metadata=None):
        """return a file object to write into the cache. The caller
        is responsibile for closing the stream
        """
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        repo_path = os.path.join(self.cache_dir, rel_path)
      
        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))
        
        sink = open(repo_path,'w+')
        upstream = self.upstream
        
        class flo:
            '''This File-Like-Object class ensures that the file is also
            sent to the upstream after it is stored in the FSCache. '''
            def __init__(self):
                pass 
            
            @property
            def repo_path(self):
                return repo_path
            
            def write(self, str_):
                sink.write(str_)
            
            def close(self):
                sink.close()
                if upstream and not upstream.readonly and not upstream.usreadonly:
                    upstream.put(repo_path, rel_path) 
                
        return flo()
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)

        if os.path.exists(repo_path):
            os.remove(repo_path)

        if self.upstream and propagate :
            self.upstream.remove(rel_path, propagate)    
            
    def clean(self):
        logger.info("Purging: {} ".format(self.cache_dir))
        Filesystem.rm_rf(self.cache_dir)
        
        if self.upstream:
            self.upstream.clean()

        
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        
        path = path.strip('/')
        
        raise NotImplementedError() 


    def public_url_f(self, public=False, expires_in=None):
        ''' Returns a function that will convert a rel_path into a public URL'''
        if self.upstream:
            upstream_f = self.upstream.public_url_f(public=public, expires_in=expires_in)
            return lambda rel_path: upstream_f(rel_path)
        else:
            cache_dir = self.cache_dir
            return lambda rel_path: 'file://{}'.format(os.path.join(cache_dir, rel_path))            


class FsLimitedCache(FsCache):
    '''A cache that transfers files to and from a remote filesystem
    
    The `FsCache` stores files in a filesystem, possily retrieving and storing
    files to an upstream cache. 
    
    When files are written , they are written through to the upstream. If a file
    is requested that does not exist, it is fetched from the upstream. 
    
    When a file is added that causes the disk usage to exceed `maxsize`, the oldest
    files are deleted to free up space. 
    
     '''

    def __init__(self, cache_dir, maxsize=10000, upstream=None):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''
        
        from databundles.dbexceptions import ConfigurationError

        self.cache_dir = cache_dir
        self.maxsize = int(maxsize * 1048578)  # size in MB
        self.upstream = upstream
        self.readonly = False
        self.usreadonly = False
        self._database = None
        
        self.use_db = True
   
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        if not os.path.isdir(self.cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self.cache_dir)) 
        
    @property
    def database_path(self):
        return  os.path.join(self.cache_dir, 'file_database.db')
        
    @property
    def database(self):
        import sqlite3
        
        if not self.use_db:
            raise Exception("Shoundn't get here")
        
        if not self._database:
            db_path = self.database_path
            
            if not os.path.exists(db_path):
                create_sql = """
                CREATE TABLE files(
                path TEXT UNIQUE ON CONFLICT REPLACE, 
                size INTEGER, 
                time REAL)
                """
                conn = sqlite3.connect(db_path)
                conn.execute(create_sql)
                conn.close()
                
            # Long timeout to deal with contention during multiprocessing use
            self._database = sqlite3.connect(db_path,60)
            
        return self._database
            
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        c = self.database.cursor()
        r = c.execute("SELECT sum(size) FROM files")
     
        try:
            size = int(r.fetchone()[0])
        except TypeError:
            size = 0
    
        return size

    def _free_up_space(self, size, this_rel_path=None):
        '''If there are not size bytes of space left, delete files
        until there is 
        
        Args:
            size: size of the current file
            this_rel_path: rel_pat to the current file, so we don't delete it. 
        
        ''' 
        
        space = self.size + size - self.maxsize # Amount of space we are over ( bytes ) for next put
        
        if space <= 0:
            return

        removes = []

        for row in self.database.execute("SELECT path, size, time FROM files ORDER BY time ASC"):

            if space > 0:
                removes.append(row[0])
                space -= row[1]
            else:
                break
  
        for rel_path in removes:
            if rel_path != this_rel_path:
                logger.debug("Deleting {}".format(rel_path)) 
                self.remove(rel_path)
            
    def add_record(self, rel_path, size):
        import time
        c = self.database.cursor()
        try:
            c.execute("insert into files(path, size, time) values (?, ?, ?)", 
                        (rel_path, size, time.time()))
            self.database.commit()
        except Exception as e:
            import dbexceptions
            
            raise dbexceptions.FilesystemError("Failed to write to cache database '{}': {}"
                                               .format(self.database_path, e.message))

    def verify(self):
        '''Check that the database accurately describes the state of the repository'''
        
        c = self.database.cursor()
        non_exist = set()
        
        no_db_entry = set(os.listdir(self.cache_dir))
        try:
            no_db_entry.remove('file_database.db')
            no_db_entry.remove('file_database.db-journal')
        except: 
            pass
        
        for row in c.execute("SELECT path FROM files"):
            path = row[0]
            
            repo_path = os.path.join(self.cache_dir, path)
        
            if os.path.exists(repo_path):
                no_db_entry.remove(path)
            else:
                non_exist.add(path)
            
        if len(non_exist) > 0:
            raise Exception("Found {} records in db for files that don't exist: {}"
                            .format(len(non_exist), ','.join(non_exist)))
            
        if len(no_db_entry) > 0:
            raise Exception("Found {} files that don't have db entries: {}"
                            .format(len(no_db_entry), ','.join(no_db_entry)))
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.cache_dir)

        return m.hexdigest()
    
    def get_stream(self, rel_path):
        p = self.get(rel_path)
        
        if not p:
            return None
        
        return open(p)
        
    
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        import shutil
        from databundles.util import bundle_file_type

        logger.debug("LC {} get looking for {}".format(self.repo_id,rel_path)) 
               
        path = os.path.join(self.cache_dir, rel_path)

        # If is already exists in the repo, just return it. 
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            logger.debug("LC {} get {} found ".format(self.repo_id, path))
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
     
        stream = self.upstream.get_stream(rel_path)
        
        if not stream:
            logger.debug("LC {} get not found in upstream ()".format(self.repo_id,rel_path)) 
            return None
        
        # Got a stream from upstream, so put the file in this cache. 
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        
        # Copy the file from the lower cache into this cache. 
        with open(path,'w') as f:
            shutil.copyfileobj(stream, f)

                
        # Since we've added a file, must keep track of the sizes. 
        size = os.path.getsize(path)
        self._free_up_space(size, this_rel_path=rel_path)
        self.add_record(rel_path, size)
        
        stream.close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        logger.debug("LC {} got return from upstream {} -> {} ".format(self.repo_id,rel_path, path)) 
        return path


    def put(self, source, rel_path, metadata=None):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key


        sink = self.put_stream(rel_path, metadata=metadata)
        
        copy_file_or_flo(source, sink)
        
        sink.close()

        return sink.repo_path

    def put_stream(self,rel_path, metadata=None):
        """return a file object to write into the cache. The caller
        is responsibile for closing the stream. Bad things happen
        if you dont close the stream
        """
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        repo_path = os.path.join(self.cache_dir, rel_path)
      
        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))
        
        sink = open(repo_path,'w+')
        upstream = self.upstream
        this = self
        class flo:
            def __init__(self):
                pass 
            
            @property
            def repo_path(self):
                return repo_path
            
            def write(self, d):
                sink.write(d)

            def writelines(self, lines):
                sink.writelines(lines)
            
            def close(self):
                sink.close()
                
                size = os.path.getsize(repo_path)
                this.add_record(rel_path, size)

                if upstream and not upstream.readonly and not upstream.usreadonly:
                    
                    upstream.put(repo_path, rel_path, metadata=metadata) 
                    # Only delete if there is an upstream 
                    this._free_up_space(size, this_rel_path=rel_path)
                    
        return flo()
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)
        
        c = self.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", (rel_path,) )
        
        if os.path.exists(repo_path):
            os.remove(repo_path)

        self.database.commit()
            
        if self.upstream and propagate :
            self.upstream.remove(rel_path, propagate)    


    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        
        path = path.strip('/')
        
        raise NotImplementedError() 

    
    def public_url_f(self, public=False, expires_in=None):
        ''' Returns a function that will convert a rel_path into a public URL'''

        if self.upstream:
            upstream_f = self.upstream.public_url_f(public=public, expires_in=expires_in)
            return lambda rel_path: upstream_f(rel_path)
        else:
            cache_dir = self.cache_dir
            return lambda rel_path: 'file://{}'.format(os.path.join(cache_dir, rel_path))     
    

class FsCompressionCache(FsCache):
    
    '''A Cache Adapter that compresses files before sending  them to
    another cache.
     '''

    def __init__(self, upstream):
        self.upstream = upstream
        self.readonly = False
        self.usreadonly = False

    @property
    def repo_id(self):
        return "c"+self.upstream.repo_id()
    
    @property
    def cache_dir(self):
        return self.upstream.cache_dir
    
    @staticmethod
    def _rename( rel_path):
        return rel_path+".gz" if not rel_path.endswith('.gz') else rel_path
    
    def get_stream(self, rel_path):
        from databundles.util import bundle_file_type
        source = self.upstream.get_stream(self._rename(rel_path))
   
        if not source:
            return None
  
        if bundle_file_type(source) == 'gzip':
            source = self.upstream.get_stream(self._rename(rel_path))
            logger.debug("CC returning {} with decompression".format(rel_path)) 
            return gzip.GzipFile(fileobj=source)
        else:
            source = self.upstream.get_stream(rel_path)
            logger.debug("CC returning {} with passthrough".format(rel_path)) 
            return source

    def get(self, rel_path):
        raise NotImplementedError("Get() is not implemented. Use get_stream()") 

    def put(self, source, rel_path, metadata=None):
        from databundles.util import bundle_file_type

        # Pass through if the file is already compressed
    
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
    
        if not metadata:
            metadata = {}
    
      
        metadata['Content-Encoding'] = 'gzip'
    
        if bundle_file_type(source) == 'gzip':
            sink = self.upstream.put_stream(rel_path, metadata = metadata)
            copy_file_or_flo(source,  sink)
        else:
            sink = self.upstream.put_stream(self._rename(rel_path), metadata = metadata)
            copy_file_or_flo(source,  gzip.GzipFile(fileobj=sink,  mode='wb'))
      
        sink.close()
    
    def put_stream(self, rel_path,  metadata=None):
        
        if not metadata:
            metadata = {}
    
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key

        metadata['Content-Encoding'] = 'gzip'
        
        sink = self.upstream.put_stream(self._rename(rel_path),  metadata=metadata)
        
        return gzip.GzipFile(fileobj=sink,  mode='wb')

    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if not self.upstream:
            raise Exception("CompressionCache must have an upstream")
        
        return self.upstream.find(query)
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        if not self.upstream:
            raise Exception("CompressionCache must have an upstream")

        # Must always propagate, since this is really just a filter. 
        self.upstream.remove(self._rename(rel_path), propagate)    

    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        return self.upstream.list(path)


    def has(self, rel_path, md5=None):
        return self.upstream.has(self._rename(rel_path), md5=md5)

    def metadata(self, rel_path):
        return self.upstream.metadata(self._rename(rel_path))

    @property
    def connection_info(self):
        '''Return reference to the connection, excluding the secret'''
        return self.upstream.connection_info
      
    @property
    def remote(self):
        '''Return a reference to an inner cache that is a remote'''
        return self.upstream.remote
      
    def public_url_f(self, public=False, expires_in=None):
        ''' Returns a function that will convert a rel_path into a public URL'''
        
        if not self.upstream:
            raise Exception("CompressionCache must have an upstream")
        
        upstream_f = self.upstream.public_url_f(public=public, expires_in=expires_in)
        rename_f = self._rename
        return lambda rel_path: upstream_f(rename_f(rel_path))
    

class S3Cache(object):
    '''A cache that transfers files to and from an S3 bucket
    
     '''

    def __init__(self, bucket=None, access_key=None, secret=None, prefix=None):
        '''Init a new S3Cache Cache

        '''
        from boto.s3.connection import S3Connection

        self.readonly = False
        self.usreadonly = False
        self.is_remote = False
        self.access_key = access_key
        self.bucket_name = bucket
        self.prefix = prefix
        self.upstream = None
        self.conn = S3Connection(self.access_key, secret, is_secure = False )
        self.bucket = self.conn.get_bucket(self.bucket_name)
  
  
    def _rename(self, rel_path):
        import re
        return re.sub('\.gz$','',rel_path)
  
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        raise NotImplementedError() 
     
    def add_record(self, rel_path, size):
        raise NotImplementedError() 

    def verify(self):
        raise NotImplementedError() 
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.bucket_name)

        return m.hexdigest()
    
    @property
    def connection_info(self):
        '''Return reference to the connection, excluding the secret'''
        
        return {'service':'s3','bucket':self.bucket_name, 'prefix':self.prefix, 'access_key': self.access_key}

    @property
    def remote(self):
        '''Return a reference to an inner cache that is a remote'''

        if self.is_remote:
            return self
        else:
            return None

    def get_stream(self, rel_path):
        """Return the object as a stream"""
        from boto.s3.key import Key
        from boto.exception import S3ResponseError 
        
        rel_path = self._rename(rel_path)
        
        import StringIO
        
        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path
        
        logger.debug("3C {} get_stream looking for {}".format(self.repo_id,rel_path)) 
        
        k = Key(self.bucket)

        k.key = rel_path
 
        b = StringIO.StringIO()
        try:
            k.get_contents_to_file(b)
            b.seek(0)
            return b;
        except S3ResponseError as e:
            if e.status == 404:
                return None
            else:
                raise e
   
        
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        
        rel_path = self._rename(rel_path)
        
        raise NotImplementedError('Should only use the stream interface. ')
    
  
    def _get_boto_key(self, rel_path):
        from boto.s3.key import Key

        rel_path = self._rename(rel_path)

        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path

        k = self.bucket.get_key(rel_path)
        
        return k        
  
    def put(self, source, rel_path,  metadata=None):  
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        rel_path = self._rename(rel_path)
        

        sink = self.put_stream(rel_path, metadata = metadata)
        
        copy_file_or_flo(source, sink)
        
        sink.close()

        return rel_path
    
    def put_key(self, source, rel_path):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''

        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        from boto.s3.key import Key

        rel_path = self._rename(rel_path)

        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path
        
        k = Key(self.bucket)
        k.key = rel_path

        try:
            k.set_contents_from_file(source)
        except AttributeError:
            if os.path.getsize(source) > 4.8*1024*1024*1024:
                # Need to do multi-part uploads here
                k.set_contents_from_filename(source)
            else:
                k.set_contents_from_filename(source)
      
    def put_stream(self, rel_path,  metadata=None):
        '''Return a flie object that can be written to to send data to S3. 
        This will result in a multi-part upload, possibly with each part
        being sent in its own thread '''

        import Queue
        import time
        import threading
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        md5 = metadata.get('md5',None) if metadata else None
        public = metadata.get('public',False) if metadata else None

        rel_path = self._rename(rel_path)

        class ThreadUploader(threading.Thread):
            """Thread class for uploading a part to S3"""
            def __init__(self, n, queue):
                threading.Thread.__init__(self)
                self.n = n
                self.queue = queue
            
            def run(self):
              
                while True:
                    mp, part_number, buf = self.queue.get()
                    if mp is None: # Signal to die
                        logger.debug("put_stream: Thread {} exiting".format(self.n))
                        self.queue.task_done()
                        return
                    logger.debug("put_stream: Thread {}: processing part: {}".format(self.n, part_number))
                    t1 = time.time()
                    try:
                        mp.upload_part_from_file(buf,part_number  )
                    finally:
                        self.queue.task_done()
                        t2 = time.time()
                        logger.debug("put_stream: Thread {}, part {}. time = {} rate =  {}b/s"
                                     .format(self.n, part_number, round(t2-t1,3), round((float(buf.tell())/(t2-t1)), 2)))
                    
                        
            
        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path

        if metadata is None:
            metadata = {}
        
        if md5:
            metadata['md5'] = md5 # Multipart uploads don't use md5 for etag
            
        this = self
        
        buffer_size = 50*1024*1024 # Min part size is 5MB
        num_threads = 4
        thread_upload_queue = Queue.Queue(maxsize=100)

        for i in range(num_threads):
            t = ThreadUploader(i, thread_upload_queue)
            t.setDaemon(True)
            t.start()

        class flo:
            '''Object that is returned to the caller, for the caller to issue
            write() or writeline() calls on '''
       
            def __init__(self):

                self.mp = this.bucket.initiate_multipart_upload(rel_path, metadata=metadata)
                self.part_number = 1;
                self.buffer = io.BytesIO()
                self.total_size = 0
     
            def _send_buffer(self):
                '''Schedules a buffer to be sent in a thread by queuing it'''
                logger.debug("_send_buffer: sending part {} to thread pool size: {}, total_size = {}"
                             .format(self.part_number, self.buffer.tell(), self.total_size))
                self.buffer.seek(0)
                thread_upload_queue.put( (self.mp, self.part_number, self.buffer) )


            def write(self, d):
              
                self.buffer.write(d) # Load the requested data into a buffer
                self.total_size += len(d)
                # After the buffer is large enough, send it, then create a new buffer. 
                if self.buffer.tell() > buffer_size:
                    self._send_buffer() 
                    
                    self.part_number += 1;
                    self.buffer = io.BytesIO()

            def writelines(self, lines):
                raise NotImplemented()
            
            def close(self):
               
                if self.buffer.tell() > 0:
                    self._send_buffer()

                thread_upload_queue.join()  # Wait for all of th upload to complete
         
                for i in range(num_threads):
                    thread_upload_queue.put( (None,None,None) ) # Tell all of the threads to die
 
                thread_upload_queue.join()  # Wait for all of the threads to exit
                             
                self.mp.complete_upload()
                
                if public:
                    this.bucket.set_acl('public-read', rel_path)
                
            
        return flo()
     
            
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if  self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        from boto.s3.key import Key
        
        rel_path = self._rename(rel_path)
        
        key = self._get_boto_key(rel_path)
        if key:
            
            key.delete()    
        
    def list(self, path=None):
        '''Get a list of all of bundle files in the cache. Does not return partition files'''
        
        path = self.prefix+'/'+path.strip('/') if path else self.prefix
        
        l = []
        for e in self.bucket.list(path):
            path = e.name.replace(self.prefix,'',1).strip('/')
            if path.startswith('_'):
                continue
            
            if path.count('/') > 1:
                continue # partition files
            
            l.append(path)
        
        return l

    def has(self, rel_path, md5=None):
        
        rel_path = self._rename(rel_path)
        
        k = self._get_boto_key(rel_path)
        
        if not md5:
            return k and k.exists()
        else:
            return (k and k.exists() and  md5 == k.get_metadata('md5'))

    def metadata(self, rel_path):
        
        rel_path = self._rename(rel_path)
        
        k = self._get_boto_key(rel_path)
        
        if not k or not k.exists():
            return None
        
        d = k.metadata
        d['size'] = k.size
        d['etag'] = k.etag       
        
        return d


    def public_url_f(self, public=False, expires_in=None):
        ''' Returns a function that will convert a rel_path into a public URL'''

        if self.prefix is not None:
            prefix = self.prefix
        else:
            prefix = ''
            
        bucket = self.bucket_name

        def public_url_f_inner(rel_path):
            
            rel_path = self._rename(rel_path)

            key =  self._get_boto_key(rel_path)
            if not key:
                raise Exception("Failed to get key for path: {}".format(rel_path))
            if public:
                key =  prefix.strip('/')+"/"+rel_path.strip('/'); 
                return "https://s3.amazonaws.com/{}/{}".format(bucket, key)
        
            else:
                if not expires_in:
                    expires_in=60
                return key.generate_url(expires_in)
            
        return public_url_f_inner
        

# Stolen from : https://bitbucket.org/fabian/filechunkio/src/79ba1388ee96/LICENCE?at=default

SEEK_SET = getattr(io, 'SEEK_SET', 0)
SEEK_CUR = getattr(io, 'SEEK_CUR', 1)
SEEK_END = getattr(io, 'SEEK_END', 2)

# A File like object that operated on a subset of another file. For use in Boto
# multipart uploads. 
class FileChunkIO(io.FileIO):
    """
    A class that allows you reading only a chunk of a file.
    """
    def __init__(self, name, mode='r', closefd=True, offset=0, bytes_=None,
        *args, **kwargs):
        """
        Open a file chunk. The mode can only be 'r' for reading. Offset
        is the amount of bytes_ that the chunks starts after the real file's
        first byte. Bytes defines the amount of bytes_ the chunk has, which you
        can set to None to include the last byte of the real file.
        """
        if not mode.startswith('r'):
            raise ValueError("Mode string must begin with 'r'")
        self.offset = offset
        self.bytes = bytes_
        if bytes_ is None:
            self.bytes = os.stat(name).st_size - self.offset
        super(FileChunkIO, self).__init__(name, mode, closefd, *args, **kwargs)
        self.seek(0)

    def seek(self, offset, whence=SEEK_SET):
        """
        Move to a new chunk position.
        """
        if whence == SEEK_SET:
            super(FileChunkIO, self).seek(self.offset + offset)
        elif whence == SEEK_CUR:
            self.seek(self.tell() + offset)
        elif whence == SEEK_END:
            self.seek(self.bytes + offset)

    def tell(self):
        """
        Current file position.
        """
        return super(FileChunkIO, self).tell() - self.offset

    def read(self, n=-1):
        """
        Read and return at most n bytes.
        """
        if n >= 0:
            max_n = self.bytes - self.tell()
            n = min([n, max_n])
            return super(FileChunkIO, self).read(n)
        else:
            return self.readall()

    def readall(self):
        """
        Read all data from the chunk.
        """
        return self.read(self.bytes - self.tell())

    def readinto(self, b):
        """
        Same as RawIOBase.readinto().
        """
        data = self.read(len(b))
        n = len(data)
        try:
            b[:n] = data
        except TypeError as err:
            import array
            if not isinstance(b, array.array):
                raise err
            b[:n] = array.array(b'b', data)
        return n

def copy_file_or_flo(input_, output):
    """ Copy a file name or file-like-object to another
    file name or file-like object"""
    import shutil 
    
    input_opened = False
    output_opened = False
    try:
        if isinstance(input_, basestring):
            input_ = open(input_,'r')
            input_opened = True
    
        if isinstance(output, basestring):
            output = open(output,'w+')   
            output_opened = True 
            
        shutil.copyfileobj(input_,  output)
    finally:
        if input_opened:
            input_.close()
            
        if output_opened:
            output.close()
        