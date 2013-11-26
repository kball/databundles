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

    e = databundles.client.exceptions.get_http_exception(response.status)
        
    if e:
        raise e(response.message)
    
class RestApi(object):
    '''Interface class for the Databundles Library REST API
    '''

    def __init__(self, url):
        '''
        '''
        
        self.url = url

    @property
    def remote(self):
        # It would make sense to cache self.remote = API(), but siesta saves the id
        # ( calls like remote.datasets(id).post() ), so we have to either alter siesta, 
        # or re-create it every call. 
        return API(self.url)


    def get_ref(self, id_or_name):
        '''Return a tuple of (rel_path, dataset_identity, partition_identity)
        for an id or name'''

        id_or_name = id_or_name.replace('/','|')

        response  = self.remote.ref(id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        return response.object
  
    def _process_get_response(self, id_or_name, response, file_path=None, uncompress=False, cb=None):
        
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
                
            if r.headers['content-encoding'] == 'gzip':
                from ..util import FileLikeFromIter   
                 # In  the requests library, iter_content will auto-decompress
                response = FileLikeFromIter(r.iter_content())
            else:
                response = r.raw

        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        if file_path:
            
            if file_path is True:
                import uuid,tempfile,os
        
                file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))  
               
            if uncompress:
                # Implement uncompression with zli, 
                # see http://pymotw.com/2/zlib/
                
                raise NotImplementedError()
               
            chunksize = 8192  
            i = 0
            
            with open(file_path,'w') as file_:
                
                chunk =  response.read(chunksize) #@UndefinedVariable
                while chunk:
                    i += 1
                    if cb:
                        cb(0,i*chunksize)
                    file_.write(chunk)
                    chunk =  response.read(chunksize) #@UndefinedVariable

            return file_path
        else:
            return response
           
    def get(self, did, pid=None):
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
        from urllib import quote_plus

        try: did = did.id_ # check if it is actualy an Identity object
        except: pass

        did = did.replace('/','|')
        pid = pid.replace('/','|') if pid else None


        if pid:
            response  = self.remote.datasets(did).partitions(pid).get()
        else:
            response  = self.remote.datasets(did).get()

        return response.object # self._process_get_response(id_or_name, response, file_path, uncompress, cb=cb)
    
    
    def get_stream_by_key(self, key, cb=None, return_meta=True):
        '''Get a stream to to the remote file. 
        
        Queries the REST api to get the URL to the file, then fetches the file
        and returns a stream, wrapping it in decompression if required. '''
        
        import requests, urllib
        from ..util.flo import MetadataFlo
        
        r1  = self.remote.key(key).get()

        location = r1.get_header('location')

        if not location:
            raise_for_status(r1)

        r = requests.get(location, verify=False, stream=True)
              
        stream = r.raw
              
        if r.headers['content-encoding'] == 'gzip':
            from ..util.sgzip import GzipFile
            stream = GzipFile(stream)
        
        return MetadataFlo(stream,r.headers)


              
    def get_partition(self, d_id_or_name, p_id_or_name, file_path=None, uncompress=False, cb=False):
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
        
        return self._process_get_response(p_id_or_name, response, file_path, uncompress, cb=cb)

                     
    def put(self, metadata):
        ''''''
        import json
        from databundles.identity import new_identity

        metadata['identity'] = json.loads(metadata['identity'])
        
        identity = new_identity(metadata['identity'])

        if identity.is_bundle:
            r =  self.remote.datasets(identity.vid_enc).post(metadata)
            raise_for_status(r)
        else:
            r =  self.remote.datasets(identity.as_dataset.vid_enc).partitions(identity.vid_enc).post(metadata)
            raise_for_status(r)

        return r

    def find(self, query):
        '''Find datasets, given a QueryCommand object'''
        from databundles.library import QueryCommand
        from databundles.identity import Identity, new_identity

        if isinstance(query, basestring):
            query = query.replace('/','|')
            response =  self.remote.datasets.find(query).get()
            raise_for_status(response)
            r = response.object


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

        return r
      
    
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
    
    

    
        