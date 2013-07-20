"""Rest interface for accessing a remote library. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from databundles.client.siesta  import API 
import databundles.client.exceptions 


class NotFound(Exception):
    pass

class RestError(Exception):
    pass

def raise_for_status(response):
    import pprint

    e = databundles.client.exceptions.get_exception(response.status)
        
    if e:
        raise e(response.message)
    
class Rest(object):
    '''Interface class for the Databundles Library REST API
    '''

    def __init__(self, url,  accounts_config=None):
        '''
        '''
        
        self.url = url
        self.accounts_config = accounts_config
        
    @property
    def remote(self):
        # It would make sense to cache self.remote = API)(, but siesta saves the id
        # ( calls like remote.datasets(id).post() ), so we have to either alter siesta, 
        # or re-create it every call. 
        return API(self.url)
        
    @property
    def connection_info(self):
        '''Return  reference to this remote, excluding the connection secret'''
        return {'service':'remote', 'url':self.url}
        
    def upload_file(self, identity, path, ci=None, force=False):
        '''Upload  file to the object_store_config's object store'''
        from databundles.util import md5_for_file
        from databundles.dbexceptions import ConfigurationError
        import json

        if ci is None:
            ci = self.remote.info().objectstore().get().object

        if ci['service'] == 's3':
            from databundles.filesystem import S3Cache, FsCompressionCache
            
            if not self.accounts_config:
                raise ConfigurationError("Remote requires S3 upload, but no account_config is set for this api")
            
            secret = self.accounts_config.get('s3',{}).get(ci['access_key'], False)
            
            if not secret:
                print self.accounts_config
                raise ConfigurationError("Didn't find key {} in configuration accounts.s3".format(ci['access_key']))

            ci['secret'] = secret
            
            del ci['service']
            fs = FsCompressionCache(S3Cache(**ci))
            #fs = S3Cache(**ci)
        else:
            raise NotImplementedError("No handler for service: {} ".format(ci))

        md5 = md5_for_file(path)
        
        if  fs.has(identity.cache_key, md5) and not force:
            return identity.cache_key
        else:
            
            metadata = {'id':identity.id_, 'identity': json.dumps(identity.to_dict()), 'name':identity.name, 'md5':md5}

            return fs.put(path, identity.cache_key,  metadata=metadata)
        
    def get_ref(self, id_or_name):
        '''Return a tuple of (rel_path, dataset_identity, partition_identity)
        for an id or name'''

        response  = self.remote.datasets.find(id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        return response.object
  
    def get(self, id_or_name, file_path=None, uncompress=False):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        from databundles.util import bundle_file_type
        from io  import BufferedReader
        try: id_or_name = id_or_name.id_ # check if it is actualy an Identity object
        except: pass

       
        response  = self.remote.datasets(id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        
        if response.status == 303 or response == 302:
            import requests

            location = response.get_header('location')
            
            r = requests.get(location, verify=False, stream=True)

            if r.status_code != 200:
                from xml.dom import minidom
                o = minidom.parse(r.raw)

                # Assuming the response is in XML because we are usually calling s3
                raise RestError("{} Error from server after redirect to {} : XML={}"
                                .format(r.status_code,location,  o.toprettyxml()))
                
            uncompress =  r.headers['content-encoding'] == 'gzip'
              
            response = r.raw
            
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        
        if file_path:
            
            if file_path is True:
                import uuid,tempfile,os
        
                file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))  
               
            chunksize = 8192  
            with open(file_path,'w') as file_:
                
                chunk =  response.read(chunksize) #@UndefinedVariable
                while chunk:
                    file_.write(chunk)
                    chunk =  response.read(chunksize) #@UndefinedVariable
    
            if uncompress:
                # Would like to use gzip as a filter, but the response only has read(), 
                # and gzip requires tell() and seek()
                import gzip
                import os
                with gzip.open(file_path) as zf, open(file_path+'_', 'wb') as of:
                    chunk = zf.read(chunksize)
                    while chunk:
                        of.write(chunk)
                        chunk = zf.read(chunksize)

                os.rename(file_path+'_', file_path)

            return file_path
        else:

            if uncompress:
                from ..util import FileLikeFromIter
                return FileLikeFromIter(r.iter_content())
            else:
            
                return r.raw
            
    def get_partition(self, d_id_or_name, p_id_or_name, file_path=None):
        '''Get a partition by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        response  = self.remote.datasets(d_id_or_name).partitions(p_id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {} / {}".format(d_id_or_name, p_id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        if file_path:
            
            if file_path is True:
                    import uuid,tempfile,os
            
                    file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                    if not os.path.exists(os.path.dirname(file_path)):
                        os.makedirs(os.path.dirname(file_path))  
               
            with open(file_path,'w') as file_:
                chunksize = 8192
                chunk =  response.read(chunksize) #@UndefinedVariable
                while chunk:
                    file_.write(chunk)
                    chunk =  response.read(chunksize) #@UndefinedVariable
    
            return file_path
        else:
            # Read the damn thing yourself ... 
            return response
                    
    def _put(self, source, identity):
        '''Put the source to the remote, creating a compressed version if
        it is not originally compressed'''
        
        from databundles.util import bundle_file_type
        import gzip
        import os, tempfile, uuid
        from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber
        
        id_ = identity.id_
        
        on = ObjectNumber.parse(id_)
 
        if not on:
            raise ValueError("Failed to parse id: '{}'".format(id_))
 
        if not  isinstance(on, (DatasetNumber, PartitionNumber)):
            raise ValueError("Object number '{}' is neither for a dataset nor partition".format(id_))
 
        type_ = bundle_file_type(source)

        if  type_ == 'sqlite' or type_ == 'hdf':
            import shutil
            # If it is a plain sqlite file, compress it before sending it. 
            try:
                cf = os.path.join(tempfile.gettempdir(),str(uuid.uuid4()))
                
                with gzip.open(cf, 'wb') as out_f:
                    try:
                        shutil.copyfileobj(source, out_f)
                    except AttributeError:
                        with open(source) as in_f:
                            shutil.copyfileobj(in_f, out_f)
                        
             
                with open(cf) as sf_:
                    if isinstance(on,DatasetNumber ):
                        response =  self.remote.datasets(id_).put(sf_)
                    else:
                        response =  self.remote.datasets(str(on.dataset)).partitions(str(on)).put(sf_)

            finally:
                if os.path.exists(cf):
                    os.remove(cf)
       
        elif type_ == 'gzip':
            # the file is already gziped, so nothing to do. 

            if isinstance(on,DatasetNumber ):
                response =  self.remote.datasets(id_).put(source)
            else:
                response =  self.remote.datasets(str(on.dataset)).partitions(str(on)).put(source)
            
        else:
            raise Exception("Bad file for id {}  got type: {} ".format(id_, type_))

        raise_for_status(response)
        
        return response


    def put_to_api(self,source, identity):
        '''Put the bundle in source to the remote library 
        Args:
            identity. An identity object that identifies the bundle or partition
            source. Either the name of the bundle file, or a file-like opbject
            
        This writes to the remote. The preferred method is to save to the object store first
            
        '''
        from sqlalchemy.exc import IntegrityError

         
        try:
            # a Filename
            with open(source) as flo:
                r =  self._put(flo, identity)
        except IntegrityError as e:
            raise e
        except Exception as e:
            # an already open file
            r =  self._put(source, identity)

        if isinstance(r.object, list):
            r.object = r.object[0]

        return r
       
    def put(self,source, identity):
            
        ci = self.remote.info().objectstore().get().object
          
        if ci['service'] == 'here':
                
            # Upload directly to the remote. 
            return self.put_to_api(source,identity)
            
        else:
              
            # Upload to the remote first, then kick the API to get it from the remote
                
            # Upload the file to the object store, outside of the API
            self.upload_file( identity, source, ci=ci)
            
            # Tell the API to check the file. 
            return self.remote.load().post(identity.to_dict())

   
    def find(self, query):
        '''Find datasets, given a QueryCommand object'''
        from databundles.library import QueryCommand
        from databundles.identity import Identity, PartitionIdentity, new_identity
        

        if isinstance(query, basestring):
            response =  self.remote.datasets.find(query).get()
            raise_for_status(response)
            r = [response.object]
            
        elif isinstance(query, dict):
            # Dict form of  QueryCOmmand
            response =  self.remote.datasets.find.post(query)
            raise_for_status(response)
            r = response.object
            
        elif isinstance(query, QueryCommand):
            response =  self.remote.datasets.find.post(query.to_dict())
            raise_for_status(response)
            r = response.object
            
        else:
            raise ValueError("Unknown input type: {} ".format(type(query)))
        
        
        raise_for_status(response)
       
    
        # Convert the result back to the form we get from the Library query 
        
        from collections import namedtuple
        Ref1= namedtuple('Ref1','Dataset Partition')
        Ref2= namedtuple('Ref2','Dataset')

        return [ new_identity(i) for i in r  if i is not False]
    
    
    def list(self):
        '''Return a list of all of the datasets in the library'''
        response =   self.remote.datasets.get()
        raise_for_status(response)
        return response.object
            
    def dataset(self, name_or_id):
        
        ref = self.get_ref(name_or_id)
        
        if not ref:
            return False
        
        id =  ref['dataset']['id']
        
        response =   self.remote.datasets(id).info().get()
        raise_for_status(response)
        return response.object
        
            
    def close(self):
        '''Close the server. Only used in testing. '''
        response =   self.remote.test.close.get()
        raise_for_status(response)
        return response.object

    
    def backup(self):
        '''Tell the server to backup its library to the remote'''
        response =   self.remote.backup.get()
        raise_for_status(response)
        return response.object
    
    

    
        