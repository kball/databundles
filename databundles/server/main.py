'''
REST Server For DataBundle Libraries. 
'''

from bottle import  error, get, put, post, request, response, redirect #@UnresolvedImport
from bottle import HTTPResponse, static_file, install, url #@UnresolvedImport
from bottle import ServerAdapter, server_names, Bottle  #@UnresolvedImport
from bottle import run, debug #@UnresolvedImport

from decorator import  decorator #@UnresolvedImport
import databundles.library 
import databundles.run
import databundles.util
from databundles.bundle import DbBundle
import logging
import os
from sqlalchemy.orm.exc import NoResultFound

import databundles.client.exceptions as exc

# This might get changed, as in test_run
run_config = databundles.run.RunConfig()
library_name = 'default'

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG)
    
    
def get_library_config(name=None):
    global library_name

    if name is not None:
        library_name = name

    cfg =  run_config.library.get(library_name)
    
    if not cfg:
        raise Exception("Failed to get exception for name {} ".format(library_name))
    
    return cfg
    
def get_library(name=None):
    '''Return the library. In a function to defer execution, so the
    run_config variable can be altered before it is called. '''
    
    global library_name
    # Originally, we were caching the library, but the library
    # holds open a sqlite database, and that isn't multi-threaded, so then
    # we can't use a multi-threaded server. 
    # Of course, then you have concurrency problems with sqlite .... 
    if name is not None:
        library_name = name

    return databundles.library._get_library(run_config, library_name)
 

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

@get('/datasets')
def get_datasets():
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    return { i.id_ : i.to_dict() for i in get_library().datasets}
   
@get('/datasets/find/<term>')
def get_datasets_find(term):
    '''Find a partition or data bundle with a, id or name term '''
    
    dataset, partition  = get_library().get_ref(term)
     
    if dataset is False:
        return False
     
    if partition:
        return partition.to_dict() 
    else:
        return dataset.to_dict()
  
@post('/datasets/find')
def post_datasets_find():
    '''Post a QueryCommand to search the library. '''
    from databundles.library import QueryCommand
   
    q = request.json
   
    bq = QueryCommand(q)
    results = get_library().find(bq)

    out = []
    for r in results:

        out.append(r.to_dict())
        
    return out

def _get_dataset_partition_record(did, pid):
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
    
    bundle =  get_library().get(did)
    
    # Need to read the file early, otherwise exceptions here
    # will result in the cilent's ocket disconnecting. 

    if not bundle:
        raise exc.NotFound('No dataset for id: {}'.format(did))

    partition = bundle.partitions.get(pid)

    return bundle,partition


def _read_body(request):
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

@put('/datasets/<did>')
@CaptureException
def put_dataset(did): 
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

    try:
        cf = _read_body(request)
     
        size = os.stat(cf).st_size
        
        if size == 0:
            raise exc.BadRequest("Got a zero size dataset file")
        
        if not os.path.exists(cf):
            raise exc.BadRequest("Non existent file")
 
        # Now we have the bundle in cf. Stick it in the library. 
        
        # We're doing these exceptions here b/c if we don't read the body, the
        # client will get an error with the socket closes. 
        try:
            on = ObjectNumber.parse(did)
        except ValueError:
            raise exc.BadRequest("Unparse dataset id: {}".format(did))
        
        if not isinstance(on, DatasetNumber):
            raise exc.BadRequest("Bad dataset id, not for a dataset: {}".format(did))
       
        # Is this a partition or a bundle?
        
        try:
            tb = DbBundle(cf)
            type = tb.db_config.info.type
        except Exception as e:
            logger.error("Failed to access database: {}".format(cf))
            raise
            
        if( type == 'partition'):
            raise exc.BadRequest("Bad data type: Got a partition")
       
        if(tb.identity.id_ != did ):
            raise exc.BadRequest("""Bad request. Dataset id of URL doesn't
            match payload. {} != {}""".format(did,tb.identity.id_))
    
        library_path, rel_path, url = get_library().put(tb) #@UnusedVariable

        identity = tb.identity
        
        # if that worked, OK to remove the temporary file. 
    finally :
        pass
        #os.remove(cf)
      
    r = identity.to_dict()
    r['url'] = url
    return r
  
@post('/load')
@CaptureException
def post_load(): 
    '''Ask the libary to check its object store for the  given dataset. We only need to do this for
    datasets, which are referenced by the interface. 

    '''
    from databundles.identity import new_identity
    
    identity = new_identity(request.json)
    
    if identity.is_bundle:
        
        l = get_library()
        
        # This will pull the file into the local cache from the remote, 
        # As long as the remote is listed as an upstream for the library. 
        path = l.cache.get(identity.cache_key)
        
        l.database.install_bundle_file(identity, path)
        
        if not l.cache.has(identity.cache_key):
            raise exc.Gone("Failed to get object {} from upstream ".format(identity.cache_key))

    return identity.to_dict()

@get('/datasets/<did>') 
@CaptureException   
def get_dataset_bundle(did):
    '''Get a bundle database file, given an id or name
    
    Args:
        id    The Name or id of the dataset bundle. 
              May be for a bundle or partition
    '''

    l = get_library()
    
    # We can get the dataset file to the local storage because we need to reference it in the interface. 
    # which is not necessary for partitions. 
    bp = l.get(did)

    if not bp:
        raise Exception("Didn't find dataset for id: {}, library = {}  ".format(did, get_library().database.dsn))

    if l.remote:
        url = l.remote.public_url_f()(bp.identity.cache_key)
        logger.debug("Redirect bundle, {}".format(url))
        redirect(url)
    else:
        
        f = bp.database.path
    
        if not os.path.exists(f):
            raise exc.NotFound("No file {} for id {}".format(f, did))
        
        logger.debug("Returning bundle directly")
        
        return static_file(f, root='/', mimetype="application/octet-stream")

@get('/datasets/:did/info')
def get_dataset_info(did):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''

    gr =  get_library().get(did)
     
    if not gr:
        raise exc.NotFound("Failed to find dataset for {}".format(did))
    
    d = {'dataset' : gr.bundle.identity.to_dict(), 'partitions' : {}}
         
    for partition in  gr.bundle.partitions:
        d['partitions'][partition.identity.id_] = partition.identity.to_dict()
    
    return d

@get('/datasets/<did>/partitions')
@CaptureException
def get_dataset_partitions_info(did):
    ''' GET    /dataset/:did/partitions''' 
    gr =  get_library().get(did)
    
    if not gr:
        raise exc.NotFound("Failed to find dataset for {}".format(did))
   
    out = {}

    for partition in  gr.bundle.partitions:
        out[partition.id_] = partition.to_dict()
        
    return out;

@get('/datasets/<did>/partitions/<pid>')
@CaptureException
def get_dataset_partitions(did, pid):
    '''Return a partition for a dataset'''
    from databundles.client.exceptions import NotFound
    from databundles.dbexceptions import NotFoundError
    
    dataset, partition = _get_dataset_partition_record(did, pid)

    if not dataset:
        logger.info("Didn't find dataset")
        raise NotFound("Didn't find dataset associated with id {}".format(did))
        
    if not partition:
        logger.info("Didn't find partition")
        raise NotFound("Didn't find partition associated with id {}".format(pid))
    
    try:
        # Realize the partition file in the top level cache. 
        r = get_library().get(partition.identity.id_)
    except NotFoundError as e:
        raise NotFound("Found partition record, but not partition in library for {}. Original Exception: {}"
                       .format(partition.identity.name, e.message))
        
    return static_file(r.partition.database.path, root='/', mimetype="application/octet-stream")    

@get('/info/objectstore')
@CaptureException
def get_info_objectstore():
    """Return information about where to upload files"""

    l = get_library()
    
    if l.remote:
        # Send it directly to the upstream API
        r = l.remote.connection_info
    else:
        # Send it here
        r = {'service':'here'}
       
    # import uuid
    # r['ticket'] = str(uuid.uuid4())  
    
    return r
  

@put('/datasets/<did>/partitions/<pid>')
@CaptureException
def put_datasets_partitions(did, pid):
    '''Upload a partition database file'''
    
    payload_file = _read_body(request)
    
    try:
        
        dataset, partition = _get_dataset_partition_record(did, pid) #@UnusedVariable
        
        library_path, rel_path, url = get_library().put_file(partition.identity, payload_file) #@UnusedVariable
        
        logger.info("Put partition {} {} to {}".format(partition.identity.id_,  partition.identity.name, library_path))   

    finally:
        if os.path.exists(payload_file):
            os.remove(payload_file)


    if not os.path.exists(library_path):
        raise Exception("Can't find {} after put".format(library_path))

    r = partition.identity.to_dict()
    r['url'] = url
    
    return r 




#### Test Code

@get('/test/echo/<arg>')
def get_test_echo(arg):
    '''just echo the argument'''
    return  (arg, dict(request.query.items()))

@put('/test/echo')
def put_test_echo():
    '''just echo the argument'''
    return  (request.json, dict(request.query.items()))


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

def test_run(config=None, library_name=None):
    '''Run method to be called from unit tests'''
    from bottle import run, debug #@UnresolvedImport
  
    # Reset the library  with a  different configuration. This is the module
    # level library, defined at the top of the module. 
    if config:
        global run_config
        run_config = config # If this is called before get_library, will change the lib config

    debug()

    l = get_library(library_name)  # fixate library
    config = get_library_config(library_name)
    port = config.get('port', 7979)
    host = config.get('host', 'localhost')
    
    logger.info("starting server on http://{}:{}".format(host, port))
    
    return run(host=host, port=port, reloader=False, server='stoppable')

def local_run(config=None, name='default', reloader=True):
 
    global stoppable_wsgi_server_run
    stoppable_wsgi_server_run = None
    
    if config:
        global run_config
        run_config = config # If this is called before get_library, will change the lib config
    
    debug()

    
    l = get_library(name)  #@UnusedVariable
    config = get_library_config(name)
    port = config.get('port', 8080)
    host = config.get('host', '0.0.0.0')
    
    logger.info("starting server  for library '{}' on http://{}:{}".format(name, host, port))

    return run(host=host, port=port, reloader=reloader)
    
def local_debug_run(name='default'):

    debug()
    l = get_library()  #@UnusedVariable
    config = get_library_config(name)
    port = config.get('port', 8080)
    host = config.get('host', '0.0.0.0')
    return run(host=host, port=port, reloader=True)

def production_run(config=None, name='default', reloader=True):

    if config:
        global run_config
        run_config = config # If this is called before get_library, will change the lib config

    l = get_library(name)  #@UnusedVariable
    config = get_library_config(name)
    port = config.get('port', 80)
    host = config.get('host', '0.0.0.0')
    
    logger.info("starting server  for library '{}' on http://{}:{}".format(name, host, port))

    return run(host=host, port=port, reloader=False, server='paste')
    
if __name__ == '__main__':
    local_debug_run()
    

