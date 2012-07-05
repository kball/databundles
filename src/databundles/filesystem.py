'''
Created on Jun 23, 2012

@author: eric
'''

import os.path

from databundles.orm import File
from contextlib import contextmanager
import zipfile
import urllib
    
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
    
    BUILD_DIR = 'build'
    DOWNLOAD_DIR = 'downloads'
    
    def __init__(self, bundle, root_directory = None):
        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_dir()
 
        if not os.path.exists(self.path(Filesystem.BUILD_DIR)):
            os.makedirs(self.path(Filesystem.BUILD_DIR),0755)
 
    @staticmethod
    def find_root_dir(testFile='bundle.yaml'):
        '''Find the parent directory that contains the bundle.yaml file '''
        import sys

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
     
        args = (self.root_directory,) +args

        p = os.path.normpath(os.path.join(*args))    
        dir_ = os.path.dirname(p)
        if not os.path.exists(dir_):
            os.makedirs(dir_)

        return p

    def build_path(self, *args):
        
        if args[0] == self.BUILD_DIR:
            raise ValueError("Adding build to existing build path "+os.path.join(*args))
        
        args = (self.BUILD_DIR,) + args
        return self.path(*args)


    def downloads_path(self, *args):
        
        if args[0] == self.DOWNLOAD_DIR:
            raise ValueError("Adding download to existing download path "+os.path.join(*args))
        
        args = (self.DOWNLOAD_DIR,) + args
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
        '''Compute hash of a file in chuncks'''
        import hashlib
        md5 = hashlib.md5()
        with open(path,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), b''): 
                md5.update(chunk)
        return md5.hexdigest()
 
 
    @contextmanager
    def unzip(self,path):
        '''Context manager to extract a single file from a zip archive, and delete
        it when finished'''
        
        extractDir = os.path.dirname(path)

        with zipfile.ZipFile(path) as zf:
            for name in  zf.namelist():
                extractFilename = os.path.join(extractDir,name)
                
                if os.path.exists(extractFilename):
                    os.remove(extractFilename)
                    
                self.bundle.log('Extracting'+extractFilename+' from '+path)
                name = name.replace('/','').replace('..','')
                zf.extract(name,extractDir )
                    
                yield extractFilename
                os.unlink(extractFilename)

    @contextmanager
    def download(self,url, **kwargs):
        '''Context manager to download a file, return it for us, 
        and delete it when done'''

        file_path = None
        try:    
            
            file_name = urllib.quote_plus(url)
            file_path = self.downloads_path(file_name)
            
            cache = kwargs.get('cache',False)
            
            if not cache or not os.path.exists(file_path):
                self.bundle.log("Downloading "+url)
                file_path, headers = urllib.urlretrieve(url,file_path) #@UnusedVariable
                
                if not os.path.exists(file_path):
                    raise Exception("Failed to download "+url)

            yield file_path
            
            if not cache:
                os.remove(file_path)
            
        except IOError as e:
            self.bundle.error("Failed to download "+url+" to "+file_path+" : "+str(e))
            raise e
            
        finally:
            if file_path and os.path.exists(file_path) and not kwargs.get('cache',False):
                os.unlink(file_path)
        
