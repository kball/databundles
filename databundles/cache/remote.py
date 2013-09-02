from . import CacheInterface, RemoteInterface, RemoteMarker, new_cache

class RestRemote(RemoteInterface):

    def __init__(self,  host,  port = None, upstream=None, **kwargs):           
        from ..client.rest import RestApi
        self.host = host
        self.port = port if port else 80
 
        self.url = 'http://{}:{}'.format(self.host, self.port)
 
        self.api = RestApi(self.url)
 
        if upstream:
            
         
            if isinstance(upstream, (CacheInterface, RemoteMarker)):
                self.upstream = upstream
            else:
                self.upstream = new_cache(upstream)
                
        else:
            self.upstream = None
        

    def path(self, rel_path, **kwargs): 
        return self.upstream.path(rel_path, **kwargs)

        
    def get(self, rel_path, cb=None): 
        
        if not self.upstream.has(rel_path):
            return None
        
        url = self.upstream.path(rel_path)
        
        return url
        
    def get_stream(self, rel_path, cb=None): 
        
        return self.api.get_stream_by_key(rel_path)

        

    def has(self, rel_path, md5=None, use_upstream=True):
        if self.upstream:
            return self.upstream.has(rel_path)
        else:
            raise NotImplementedError()
   
    def put_bundle(self, bundle):
        
        metadata = bundle.identity.to_meta(file=bundle.database.path )
        
        return self.put(bundle.database.path, 
                         bundle.identity.cache_key,
                         metadata = metadata)
                         
    def put_partition(self, partition):
        
        metadata = partition.identity.to_meta(file=partition.database.path )
        
        return self.put(partition.database.path, 
                         partition.identity.cache_key,
                         metadata = metadata)
   
   
   
    def put(self, source, rel_path, metadata=None):
        
        from databundles.bundle import DbBundle
        from databundles.util import md5_for_file
        import json
        from ..dbexceptions import ConfigurationError

        if not self.upstream:
            raise ConfigurationError("Can't put to a RestRemote without an s3 upstream")
     
        if not metadata:
            raise ConfigurationError("Must have metadata")

        if set(['id','identity','name','md5']) != set(metadata.keys()):
            raise ConfigurationError("Metadata is missing keys: {}".format(metadata.keys()))

        # Store the bundle into the S3 cache. 
        
        if not self.upstream.has(rel_path, md5=metadata['md5']):
            r =  self.upstream.put(source, rel_path, metadata=metadata)
        else:
            r = self.upstream.path(rel_path)
            
  
        self.api.put(metadata)

        return r

    def put_stream(self,rel_path, metadata=None): 
        from io import IOBase
        import requests
        
        if set(['id','identity','name','md5']) not in set(metadata.keys()):
            raise ValueError("Must have complete metadata to use put_stream(): 'id','identity','name','md5' ") 
    
        class flo(IOBase):

            def __init__(self, api, url, upstream,  rel_path):
                
                self._api = api
                self._url = url
                self._upstream = upstream
                self._rel_path = rel_path
                self._sink = self.upstream.put_stream(rel_path, metadata=metadata) 

            def write(self, str_):
                self._sink.write(str_)
            
            def close(self):
                if not self._sink.closed:
                    self._sink.close()
                    self.api.put(self._url, metadata)
  

        return flo(self.api, self.url, self.upstream,  rel_path)
        
    
    def remove(self,rel_path, propagate = False): 
        
        self.upstream.remove(rel_path, propagate)
        
    
    def metadata(self,rel_path):
        if self.upstream:
            return self.upstream.metadata(rel_path)
        else:
            raise NotImplementedError()
    
    def find(self,query): raise NotImplementedError()
    
    def list(self, path=None,with_metadata=False): 
        
        return  self.api.list()

        path = self.prefix+'/'+path.strip('/') if path else self.prefix
        
        l = {}
        for e in self.bucket.list(path):
            path = e.name.replace(self.prefix,'',1).strip('/')
            if path.startswith('_'):
                continue
            
            if path.count('/') > 1:
                continue # partition files
            
            if with_metadata:
                d = self.metadata(path)
            else:
                d = {}
            
            l[path] = d

        return l
        
        
    def get_upsream(self, type_):
        ''''''
         
        return self._upstream  
       
    def __repr__(self):
        return "RestRemote: url={}  upstream=({})".format(self.url, self.upstream)
       
       