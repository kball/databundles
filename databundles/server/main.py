'''
REST Server For DataBundle Libraries. 
'''


from bottle import  error, hook, get, put, post, request, response, redirect #@UnresolvedImport
from bottle import HTTPResponse, static_file, install, url #@UnresolvedImport
from bottle import ServerAdapter, server_names, Bottle  #@UnresolvedImport
from bottle import run, debug #@UnresolvedImport

from decorator import  decorator #@UnresolvedImport
from  databundles.library import new_library
import databundles.util
from databundles.bundle import DbBundle
import logging
import os
from sqlalchemy.orm.exc import NoResultFound

import databundles.client.exceptions as exc

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG)

#
# The LibraryPlugin allows the library to be inserted into a reuest handler with a
# 'library' argument. 
class LibraryPlugin(object):
    
    def __init__(self, library_creator, keyword='library'):

        self.library_creator = library_creator
        self.keyword = keyword

    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect

        # Override global configuration with route-specific values.
        conf = context['config'].get('library') or {}
        
        #library = conf.get('library', self.library_creator())

        keyword = conf.get('keyword', self.keyword)
        
        # Test if the original callback accepts a 'library' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(context['callback'])[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):

            #
            # NOTE! Creating the library every call. This is bacuase the Sqlite driver
            # isn't multi-threaded. 
            #
            kwargs[keyword] = self.library_creator()

            rv = callback(*args, **kwargs)

            return rv

        # Replace the route callback with the wrapped one.
        return wrapper
 

def capture_return_exception(e):
    
    import sys
    import traceback
    
    (exc_type, exc_value, exc_traceback) = sys.exc_info() #@UnusedVariable
    
    tb_list = traceback.format_list(traceback.extract_tb(sys.exc_info()[2]))
    
    return {'exception':
     {'class':e.__class__.__name__, 
      'args':e.args,
      'trace': "\n".join(tb_list)
     }
    }   

def _CaptureException(f, *args, **kwargs):
    '''Decorator implementation for capturing exceptions '''

    try:
        r =  f(*args, **kwargs)
    except HTTPResponse:
        raise # redirect() uses exceptions
    except Exception as e:
        r = capture_return_exception(e)
        if hasattr(e, 'code'):
            response.status = e.code

    return r

def CaptureException(f, *args, **kwargs):
    '''Decorator to capture exceptions and convert them
    to a dict that can be returned as JSON ''' 

    return decorator(_CaptureException, f) # Preserves signature

class AllJSONPlugin(object):
    '''A copy of the bottle JSONPlugin, but this one tries to convert
    all objects to json ''' 
    
    from json import dumps as json_dumps
    
    name = 'json'
    remote  = 2

    def __init__(self, json_dumps=json_dumps):
        self.json_dumps = json_dumps

    def apply(self, callback, context):
      
        dumps = self.json_dumps
        if not dumps: return callback
        def wrapper(*a, **ka):
            rv = callback(*a, **ka)

            if isinstance(rv, HTTPResponse ):
                return rv
            
            #Attempt to serialize, raises exception on failure
            try:
                json_response = dumps(rv)
            except Exception as e:
                r =  capture_return_exception(e)
                json_response = dumps(r)
                
            #Set content type only if serialization succesful
            response.content_type = 'application/json'
            return json_response
        return wrapper

install(AllJSONPlugin())

@error(404)
@CaptureException
def error404(error):
    raise exc.NotFound("For url: {}".format(repr(request.url)))

@error(500)
def error500(error):
    raise exc.InternalError("For Url: {}".format(repr(request.url)))

@hook('after_request')
def close_library_db():
    pass

def _host_port(library):
    return  'http://{}{}'.format(library.host, ':'+str(library.port) if library.port != 80 else '')


@get('/datasets')
def get_datasets(library):
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    from databundles.filesystem import RemoteMarker
    
    remote = library.remote.get_upstream(RemoteMarker)
    l = library
  
    return { i.identity.cache_key : { 
                                     'identity': i.identity.to_dict() ,
                                     'urls': {
                                              'info': "{}/datasets/{}".format(_host_port(library), i.identity.vid_enc),
                                              'file': "{}/datasets/{}/db".format(_host_port(library), i.identity.vid_enc)
                                              }
                                     } 
            for i in library.datasets}
   
@get('/datasets/find/<term>')
def get_datasets_find(term, library):
    '''Find a partition or data bundle with a, id or name term '''
    from databundles.library import QueryCommand
    
    dataset, partition  = library.get_ref(term)
     
    if dataset is False:
        return False
     
    # if found, find again to put it in the same for as the
    # POST version, which uses the find() method. 

    if partition:
        qc =  QueryCommand().partition(vid=partition.vid)
    else:
        qc =  QueryCommand().identity(vid=dataset.vid)
  
    results = library.find(qc)


    out = []
    for r in results:
        out.append(r)
        
    return out
    
  
@post('/datasets/find')
def post_datasets_find(library):
    '''Post a QueryCommand to search the library. '''
    from databundles.library import QueryCommand
   
    q = request.json
   
    bq = QueryCommand(q)
    results = library.find(bq)


    out = []
    for r in results:
        out.append(r)
        
    return out

@post('/datasets/<did>')
@CaptureException
def post_dataset(did,library): 
    '''Accept a payload that describes a bundle in the remote. Download the
    bundle and install it. '''

    did = did.replace('|','/')

    from databundles.identity import new_identity, Identity
    from databundles.util import md5_for_file
    
    payload = request.json
    identity = new_identity(payload['identity'])

    if not did in set([identity.id_, identity.vid]):
        raise exc.Conflict("Dataset address '{}' doesn't match payload id '{}'".format(did, identity.vid))

    # need to go directly to remote, not library.get() because the
    # dataset hasn't been loaded yet. 
    db_path = library.load(identity.cache_key)

    if not db_path:
        logger.error("Failed to get {} from cache while posting dataset".format(identity.cache_key))
        logger.error("  cache =  {}".format(library.cache))
        logger.error("  remote = {}".format(library.remote))
        raise exc.NotFound("Didn't  get bundle file for cache key {} ".format(identity.cache_key))

    logger.debug("Loading {} for identity {} ".format(db_path, identity))

    b = DbBundle(db_path, logger=logger)

    md5 = md5_for_file(db_path)
    
    print md5
    print payload
    
    if md5 != payload['md5']:
        logger.debug('MD5 Mismatch: {} != {} '.format( md5 , payload['md5']))
        # First, try deleting the cached copy and re-fetching
        # but don't delete it unless there is an intervening cache
        #if library.remote.path(identity.cache_key).startswith('http'):
        #    raise exc.Conflict("MD5 Mismatch (a)")
        
        library.remote.remove(identity.cache_key)
        db_path = library.remote.get(identity.cache_key)
        
        md5 = md5_for_file(db_path)
        if md5 != payload['md5']:
            logger.debug('MD5 Mismatch, persiting ater refetch: {} != {} '.format( md5 , payload['md5']))
            raise exc.Conflict("MD5 Mismatch (b)")

        b = DbBundle(db_path)

    if b.identity.cache_key != identity.cache_key:
        logger.debug("Identity mismatch while posting dataset: {} != {}".format(b.identity.cache_key, identity.cache_key))
        raise exc.Conflict("Identity of downloaded bundle doesn't match request payload")

    library.put(b)

    library.run_dumper_thread()

    return b.identity.to_dict()
  

@get('/datasets/<did>') 
@CaptureException   
def get_dataset(did, library):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''
    from databundles.filesystem import RemoteMarker

    did = did.replace('|','/')

    gr =  library.get(did)
     
    if not gr:
        raise exc.NotFound("Failed to find dataset for {}".format(did))
    
    # COnstruct the response
    d = {'dataset' : gr.identity.to_dict(), 'partitions' : {}}
         
    file = library.database.get_file_by_ref(gr.identity.vid)
    
    # Get direct access to the cache that implements the remote, so
    # we can get a URL with path()
    remote = library.remote.get_upstream(RemoteMarker)
    if remote:
        d['dataset']['url'] = "{}/datasets/{}/db".format(_host_port(library), gr.identity.vid_enc)
    
    if file:
        d['dataset']['file'] = file.to_dict()

    for partition in  gr.partitions:
        d['partitions'][partition.identity.id_] = partition.identity.to_dict()
    
        file = library.database.get_file_by_ref(partition.identity.vid)
        
        if file:

            fd = file.to_dict()
            d['partitions'][partition.identity.id_]['file']  = { k:v for k,v in fd.items() if k in ['state'] }

        if remote:
            d['partitions'][partition.identity.id_]['url'] ="{}/datasets/{}/partitions/{}/db".format(_host_port(library), gr.identity.vid_enc, partition.identity.vid_enc)
        
    return d

@post('/datasets/<did>/partitions/<pid>') 
@CaptureException   
def post_partition(did, pid, library):
    from databundles.identity import new_identity, Identity
    from databundles.util import md5_for_file

    did = did.replace('|','/')
    pid = pid.replace('|','/')

    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))

    payload = request.json
    identity = new_identity(payload['identity'])

    p = b.partitions.get(pid)
    
    if not p:
        raise exc.NotFound("No partition for {} in dataset {}".format(pid, did))

    if not pid in set([identity.id_, identity.vid]):
        raise exc.Conflict("Partition address '{}' doesn't match payload id '{}'".format(pid, identity.vid))

    library.database.add_remote_file(identity)

    return identity.to_dict()

@get('/datasets/<did>/db') 
@CaptureException   
def get_dataset_file(did, library):
    from databundles.filesystem import RemoteMarker
    
    did = did.replace('|','/')

    dataset, _ = library.get_ref(did)
    
    if not dataset:
        raise exc.NotFound("No dataset found for identifier '{}' ".format(did))
    
    remote = library.remote.get_upstream(RemoteMarker)
    
    if not remote:
        raise exc.InternalError("No remote configured")
   
  
    url =  remote.path(dataset.cache_key)   
    
    return redirect(url)
  
def _get_ct(typ):
    ct = ({'application/json':'json',
          'application/x-yaml':'yaml',
          'text/x-yaml':'yaml',
          'text/csv':'csv'}
          .get(request.headers.get("Content-Type"), None))
    
    if ct is None:
        try:
            _, ct = typ.split('.',2)
        except: 
            ct = 'json'
        
    return ct
    
@get('/key/<key:path>') 
@CaptureException   
def get_key(key, library):
    from databundles.filesystem import RemoteMarker
    
    remote = library.remote.get_upstream(RemoteMarker)
    
    if not remote:
        raise exc.InternalError("No remote configured")
   
    try:
        url =  remote.path(key)   
    except AttributeError:
        raise exc.NotFound("No object for key: {}".format(key))
    return redirect(url)    
    
  
@get('/datasets/<did>/<typ:re:schema\\.?.*>') 
@CaptureException   
def get_dataset_schema(did, typ, library):
    from databundles.filesystem import RemoteMarker
    
    ct = _get_ct(typ)

    did = did.replace('|','/')
 
    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))


    if ct == 'csv':
        from StringIO import StringIO
        output = StringIO()
        response.content_type = 'text/csv'
        b.schema.as_csv(output)
        static_file(output)
    elif ct == 'json':
        import json
        s = b.schema.as_struct()
        return s
    elif ct == 'yaml': 
        import yaml 
        s = b.schema.as_struct()
        response.content_type = 'application/x-yaml'
        return  yaml.dump(s)
    else:
        raise Exception("Unknown format" )   
    
    
@get('/datasets/<did>/partitions/<pid>/db') 
@CaptureException   
def get_partition_file(did, pid, library):
    from databundles.filesystem import RemoteMarker
    from databundles.identity import new_identity, Identity
    
    did = did.replace('|','/')
    pid = pid.replace('|','/')

    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))

    payload = request.json
    identity = new_identity(payload['identity'])

    p = b.partitions.get(pid)

    if not p:
        raise exc.NotFound("No partition found for identifier '{}' ".format(pid))

    remote = library.remote.get_upstream(RemoteMarker)
    
    if not remote:
        raise exc.InternalError("No remote configured")
   
  
    url =  remote.path(p.identity.cache_key)   
    
    return redirect(url)


def _read_body(request):
    '''Read the body of a request and decompress it if required '''
    # Really important to only call request.body once! The property method isn't
    # idempotent!
    import zlib
    import uuid # For a random filename. 
    import tempfile
            
    tmp_dir = tempfile.gettempdir()
    #tmp_dir = '/tmp'
            
    file_ = os.path.join(tmp_dir,'rest-downloads',str(uuid.uuid4())+".db")
    if not os.path.exists(os.path.dirname(file_)):
        os.makedirs(os.path.dirname(file_))  
        
    body = request.body # Property acessor
    
    # This method can recieve data as compressed or not, and determines which
    # from the magic number in the head of the data. 
    data_type = databundles.util.bundle_file_type(body)
    decomp = zlib.decompressobj(16+zlib.MAX_WBITS) # http://stackoverflow.com/a/2424549/1144479
 
    if not data_type:
        raise Exception("Bad data type: not compressed nor sqlite")
 
    # Read the file directly from the network, writing it to the temp file,
    # and uncompressing it if it is compressesed. 
    with open(file_,'w') as f:

        chunksize = 8192
        chunk =  body.read(chunksize) #@UndefinedVariable
        while chunk:
            if data_type == 'gzip':
                f.write(decomp.decompress(chunk))
            else:
                f.write(chunk)
            chunk =  body.read(chunksize) #@UndefinedVariable   

    return file_

#### Test Code

@get('/test/echo/<arg>')
def get_test_echo(arg):
    '''just echo the argument'''
    return  (arg, dict(request.query.items()))

@put('/test/echo')
def put_test_echo():
    '''just echo the argument'''
    return  (request.json, dict(request.query.items()))


@get('/test/info')
def get_test_info(library):
    '''Info about the server'''
    return  str(type(library))



@get('/test/exception')
@CaptureException
def get_test_exception():
    '''Throw an exception'''
    raise Exception("throws exception")


@put('/test/exception')
@CaptureException
def put_test_exception():
    '''Throw an exception'''
    raise Exception("throws exception")


@get('/test/isdebug')
def get_test_isdebug():
    '''eturn true if the server is open and is in debug mode'''
    try:
        global stoppable_wsgi_server_run
        if stoppable_wsgi_server_run is True:
            return True
        else: 
            return False
    except NameError:
        return False

@post('/test/close')
@CaptureException
def get_test_close():
    '''Close the server'''
    global stoppable_wsgi_server_run
    if stoppable_wsgi_server_run is not None:
        logger.debug("SERVER CLOSING")
        stoppable_wsgi_server_run = False
        return True
    
    else:
        raise exc.NotAuthorized("Not in debug mode, won't close")


class StoppableWSGIRefServer(ServerAdapter):
    '''A server that can be stopped by setting the module variable
    stoppable_wsgi_server_run to false. It is primarily used for testing. '''
    
    def run(self, handler): # pragma: no cover
        global stoppable_wsgi_server_run
        stoppable_wsgi_server_run = True
   
        from wsgiref.simple_server import make_server, WSGIRequestHandler
        if self.quiet:
            class QuietHandler(WSGIRequestHandler):
                def log_request(*args, **kw): pass #@NoSelf
            self.options['handler_class'] = QuietHandler
        srv = make_server(self.host, self.port, handler, **self.options)
        while stoppable_wsgi_server_run:
            srv.handle_request()

server_names['stoppable'] = StoppableWSGIRefServer

def test_run(config):
    '''Run method to be called from unit tests'''
    from bottle import run, debug #@UnresolvedImport
  
    debug()

    port = config['port'] if config['port'] else 7979
    host = config['host'] if config['host'] else 'localhost'
    
    logger.info("starting test server on http://{}:{}".format(host, port))
    
    lf = lambda: new_library(config, True)  
    
    l = lf()
    l.database.create()
    
    install(LibraryPlugin(lf))
    
    return run(host=host, port=port, reloader=False, server='stoppable')

def local_run(config, reloader=True):
 
    global stoppable_wsgi_server_run
    stoppable_wsgi_server_run = None

    debug()

    lf = lambda:  new_library(config, True)  

    l = lf()
    l.database.create()
    
    logger.info("starting local server for library '{}' on http://{}:{}".format(l.name, l.host, l.port))

    install(LibraryPlugin(lf))
    return run(host=l.host, port=l.port, reloader=reloader)
    
def local_debug_run(config):

    debug()
    lf = lambda: new_library(config, True)  

    install(LibraryPlugin(lf))
    
    l = lf()
    l.database.create()
    
    return run(host=l.host, port=l.port, reloader=True)

def production_run(config, reloader=False):

    lf = lambda:  new_library(config, True)  


    l = lf()
    l.database.create()

    logger.info("starting production server for library '{}' on http://{}:{}".format(l.name, l.host, l.port))

    install(LibraryPlugin(lf))

    return run(host=l.host, port=l.port, reloader=reloader, server='paste')
    
if __name__ == '__main__':
    local_debug_run()
    

