"""

Server application for assigning dataset numbers. Requires a redis instance for
data storage.

Run with: python -mdatabundles.server.numbers

Requires a run_config configuration item:

numbers:
    host: gala
    port: 7977
    redis:
        host: redis
        port: 6379

For Clients:

numbers:
    key: this-is-a-long-uid-key


The key is a secret key that the client will use to assign an assignment class.
The two classes are 'authoritative' and 'registered' Only central authority operators
( like Clarinova ) should use the authoritative class. Other users can use the
'registered' class. Without a key and class assignment, the callers us the 'unregistered' class.

Set the assignment class with the redis-cli:

    set assignment_class:this-is-a-long-uid-key authoritative

There is only one uri to call:

    /next

It returns a JSON dict, with the 'number' key mapping to the number.

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from bottle import  error, hook, get, put, post, request, response, redirect
from bottle import HTTPResponse, static_file, install, url
from bottle import  Bottle
from bottle import run, debug #@UnresolvedImport

from decorator import decorator #@UnresolvedImport
import logging

import databundles.client.exceptions as exc
import databundles.util
import redis

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG)





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

class RedisPlugin(object):

    def __init__(self, pool, keyword='redis'):

        self.pool = pool
        self.keyword = keyword

    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect

        # Override global configuration with route-specific values.
        conf = context['config'].get('redis') or {}

        keyword = conf.get('keyword', self.keyword)

        # Test if the original callback accepts a 'library' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(context['callback'])[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):

            kwargs[keyword] = redis.Redis(connection_pool=self.pool)

            rv = callback(*args, **kwargs)

            return rv

        # Replace the route callback with the wrapped one.
        return wrapper

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

            if isinstance(rv, basestring ):
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
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'
    


@get('/')
def get_root(redis):

    return []


def request_delay(nxt,delay,delay_factor):
    """
    Calculate how long this client should be delayed before next
    request.
    :rtype : object
    """

    import time

    now = time.time()

    try:
        delay = float(delay)
    except:
        delay = 1.0

    nxt = float(nxt) if nxt else now-1

    since = None
    if now <= nxt:
        # next is in the future, so the
        # request is rate limited

        ok = False

    else:
        # next is in the past, so the request can proceed
        since = now - nxt

        if since > 2*delay:

            delay = int(delay / delay_factor )

            if delay < 1:
                delay = 1

        else:

            delay = int(delay * delay_factor)

        if nxt < now:
            nxt = now

        nxt = nxt + delay

        ok = True

    return ok, since, nxt,  delay, nxt-now, (nxt+4*delay)-now


@get('/next')
@CaptureException
def get_test(redis):
    from time import time
    from databundles.identity import DatasetNumber

    delay_factor = 2

    ip = str(request.remote_addr)
    now = time()

    next_key = "next:"+ip
    delay_key = "delay:"+ip
    ipallocated_key = "allocated:"+ip


    #
    # The assignment class determine how long the resulting number will be
    # which namespace the number is drawn from, and whether the user is rate limited
    # The assignment_class: key is assigned and set externally
    #
    access_key = request.query.access_key

    assignment_class = None
    if access_key:
        assignment_class_key = "assignment_class:"+access_key
        assignment_class = redis.get(assignment_class_key )

    if not assignment_class:
        raise exc.NotAuthorized('Use an access key to gain access to this service')

    # The number space depends on the assignment class.

    number_key = "dataset_number:"+assignment_class
    authallocated_key = "allocated:"+assignment_class

    nxt = redis.get(next_key)
    delay = redis.get(delay_key)

    # Adjust rate limiting based on assignment class
    if assignment_class == 'authoritative':
        since, nxt, delay, wait, safe  = (0,now-1,0,0,0)

    elif assignment_class == 'registered':
        delay_factor = 1.1

    ok, since, nxt, delay, wait, safe = request_delay(nxt,delay,delay_factor)

    with redis.pipeline() as pipe:
        redis.set(next_key, nxt)
        redis.set(delay_key, delay)

    logger.info("ip={} ok={} since={} nxt={} delay={} wait={} safe={}"
                    .format(ip, ok, since, nxt, delay, wait, safe))

    if ok:
        number = redis.incr(number_key)

        dn = DatasetNumber(number, None, assignment_class)

        redis.sadd(ipallocated_key, dn)
        redis.sadd(authallocated_key, dn)

    else:
        number = None
        raise exc.TooManyRequests("Requests will resume in {} seconds".format(wait))

    return dict(ok=ok,
                number=str(dn),
                assignment_class=assignment_class,
                wait=wait,
                safe_wait=safe,
                nxt=nxt,
                delay=delay)


@get('/echo/<term>')
def get_echo_term(term, redis):
    '''Test function to see if the server is working '''

    return [term]

def _run(host, port, unregistered_key,  reloader=False, **kwargs):

    redis_config = kwargs.get('redis')

    pool = redis.ConnectionPool(host=redis_config['host'],
                                port=redis_config['port'], db=0)

    rds = redis.Redis(connection_pool=pool)

    # This is the key that can be distributed publically. It is only to
    # keep bots and spiders from sucking up a bunch of numbers.
    rds.set("assignment_class:"+unregistered_key,'unregistered')

    install(RedisPlugin(pool))

    return run( host=host, port=port, reloader=reloader, server='paste')
    
if __name__ == '__main__':
    import argparse
    from databundles.run import  get_runconfig
    rc = get_runconfig()

    ng = rc.group('numbers')

    redis_config = ng['redis']

    d = rc.group('numbers')

    parser = argparse.ArgumentParser(prog='python -mdatabundles.server.numbers',
                                     description='Run an Ambry numbers server')

    parser.add_argument('-H','--host', default=None, help="Server host. Defaults to configured value: {}".format(d['host']))
    parser.add_argument('-p','--port', default=None, help="Server port. Defaults to configured value: {}".format(d['port']))

    args = parser.parse_args()

    if args.port:
        d['port'] = args.port

    if args.host:
        d['host'] = args.host

    _run(**d)
    

