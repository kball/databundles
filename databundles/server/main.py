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
    
    def __init__(self, library, keyword='library'):

        self.library = library
        self.keyword = keyword
    
    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect
        
        # Override global configuration with route-specific values.
        conf = context['config'].get('library') or {}
        
        library = conf.get('library', self.library)

        keyword = conf.get('keyword', self.keyword)
        
        # Test if the original callback accepts a 'library' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(context['callback'])[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):

            kwargs[keyword] = library

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
    '''Close the library database after the request if is sqlite, since sqlite
    isn't multi-threaded'''

@get('/datasets')
def get_datasets(library):
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    return { i.identity.cache_key : { 'identity': i.identity.to_dict() } for i in library.datasets}
   
@get('/datasets/find/<term>')
def get_datasets_find(term, library):
    '''Find a partition or data bundle with a, id or name term '''
    
    dataset, partition  = library.get_ref(term)
     
    if dataset is False:
        return False
     
    if partition:
        return partition.to_dict() 
    else:
        return dataset.to_dict()
  
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

def _get_dataset_partition_record(library, did, pid):
    """Get the reference information for a partition from the bundle database"""
    
    from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber
    
    don = ObjectNumber.parse(did)
    if not don or not isinstance(don, DatasetNumber):
        raise exc.BadRequest('Dataset number {} is not valid'.format(did))
  
    pon = ObjectNumber.parse(pid)
    if not pon or not isinstance(pon, PartitionNumber):
        raise exc.BadRequest('Partition number {} is not valid'.format(pid))
    
    if str(pon.dataset) != str(don):
        raise exc.BadRequest('Partition number {} does not belong to datset {}'.format(pid, did))
    
    bundle =  library.get(did)
    
    # Need to read the file early, otherwise exceptions here
    # will result in the cilent's ocket disconnecting. 

    if not bundle:
        raise exc.NotFound('No dataset for id: {}'.format(did))

    partition = bundle.partitions.get(pid)

    return bundle,partition


@put('/datasets/<did>')
@CaptureException
def put_dataset(did, library): 
    
    '''Store a reference to a dataset. The library will fetch the dataset fro mthe object store and
    install it. '''
    
    '''Store a bundle, calling put() on the bundle file in the Library.
    
        :param did: A dataset id string. must be parsable as a `DatasetNumber`
        value
        :rtype: string
        
        :param pid: A partition id string. must be parsable as a `partitionNumber`
        value
        :rtype: string
        
        :param payload: The bundle database file, which may be compressed. 
        :rtype: binary
    
    '''
    from databundles.identity import ObjectNumber, DatasetNumber
    import stat

    from databundles.identity import Identity

    identity = Identity.parse_name(did)

    if identity.revision == None:
        raise exc.BadRequest("Identity name must include revision")
    
    ### Only get HEAD to verify that caller was able to store the dataset properly
    ### Defer downloading the data until it is requested. 
    
    if identity.is_bundle:
        
        l = library
        
        # This will pull the file into the local cache from the remote, 
        # As long as the remote is listed as an upstream for the library. 
        logger.debug("Get bundle for cache key {}".format(identity.cache_key))
        path = l.cache.get(identity.cache_key)
        l.run_dumper_thread()
        
        if not path or not os.path.exists(path):
            raise exc.Gone("Failed to get object {} from upstream; path '{}' does not exist. Cache connection = {} ".format(identity.cache_key, path, l.cache.connection_info))
        
        logger.debug("Installing path {} to identity {}".format(path, identity  ))
        
        l.database.install_bundle_file(identity, path)
        
        logger.debug("Intstalled path {} to identity {}".format(path, identity  ))
        
        if not l.cache.has(identity.cache_key):
            raise exc.Gone("Failed to get object {} from upstream; cache doesn't have key as after install ".format(identity.cache_key))

    else:
        # Don't need to load non bundles, because they don't have data that gets loaded into the library databases
        pass
    
    return identity.to_dict()
      
    #r = identity.to_dict()
    #r['url'] = url
    #l.run_dumper_thread()
    #return r
  

@get('/datasets/<did>') 
@CaptureException   
def get_dataset(did, library):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''

    gr =  library.get(did)
     
    if not gr:
        raise exc.NotFound("Failed to find dataset for {}".format(did))
    
    d = {'dataset' : gr.identity.to_dict(), 'partitions' : {}}
         
    file = library.database.get_file_by_ref(gr.identity.vid)
    
    if file:
        d['dataset']['file'] = file.to_dict()

    for partition in  gr.partitions:
        d['partitions'][partition.identity.id_] = partition.identity.to_dict()
    
        file = library.database.get_file_by_ref(partition.identity.vid)
        
        if file:
            d['partitions'][partition.identity.id_]['file'] = file.to_dict()
        
    return d


@get('/datasets/<did>/partitions/<pid>')
@CaptureException
def get_dataset_partitions( did, pid, library):
    '''Return a partition for a dataset'''
    from databundles.client.exceptions import NotFound
    from databundles.dbexceptions import NotFoundError
    
    dataset, partition = _get_dataset_partition_record(library, did, pid)

    if not dataset:
        logger.info("Didn't find dataset")
        raise NotFound("Didn't find dataset associated with id {}".format(did))
        
    if not partition:
        logger.info("Didn't find partition")
        raise NotFound("Didn't find partition associated with id {}".format(pid))
    
    if library.remote:
        try:
            url = library.remote.public_url_f()(partition.identity.cache_key)
        except NotFoundError as e:
            raise NotFound("Found partition record, but not partition in remote for {}. Original Exception: {}"
                           .format(partition.identity.name, e.message))
            
        logger.debug("Redirect partition, {}".format(url))
        redirect(url)
    else:
        
        try:
            # Realize the partition file in the top level cache. 
            r = library.get(partition.identity.id_)
        except NotFoundError as e:
            raise NotFound("Found partition record, but not partition in library for {}. Original Exception: {}"
                           .format(partition.identity.name, e.message))
        
        logger.debug("Send file directly, {}".format(url))
        return static_file(r.partition.database.path, root='/', mimetype="application/octet-stream")    


@put('/datasets/<did>/partitions/<pid>')
@CaptureException
def put_datasets_partitions(did, pid, library):
    '''Record that a partition has been stored at in the object store'''

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
    
    install(LibraryPlugin(config))
    
    return run(host=host, port=port, reloader=False, server='stoppable')

def local_run(config, library_name='default', reloader=True):
 
    global stoppable_wsgi_server_run
    stoppable_wsgi_server_run = None

    debug()

    l = new_library(config.library(library_name))  

    logger.info("starting local server for library '{}' on http://{}:{}".format(library_name, l.host, l.port))

    install(LibraryPlugin(l))
    return run(host=l.host, port=l.port, reloader=reloader)
    
def local_debug_run(config, library_name='default'):

    debug()
    l = new_library(config.library(library_name))  

    install(LibraryPlugin(config, library_name))
    return run(host=l.host, port=l.port, reloader=True)

def production_run(config, library_name='default', reloader=False):

    l = new_library(config.library(library_name))  

    logger.info("starting production server for library '{}' on http://{}:{}".format(library_name, l.host, l.port))

    install(LibraryPlugin(l))

    return run(host=l.host, port=l.port, reloader=reloader, server='paste')
    
if __name__ == '__main__':
    local_debug_run()
    

