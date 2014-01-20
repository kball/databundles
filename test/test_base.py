"""
Created on Jun 22, 2012

@author: eric
"""
import unittest
from  testbundle.bundle import Bundle
from databundles.identity import * #@UnusedWildImport
import time, logging
import databundles.util
from databundles.run import  RunConfig

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 
logging.captureWarnings(True)

class TestBase(unittest.TestCase):

    server_url = None

    def copy_or_build_bundle(self):
        """Set up a clean bundle build, either by re-building the bundle, or
        by copying it from a saved bundle directory """
        
        # For most cases, re-set the bundle by copying from a saved version. If
        # the bundle doesn't exist and the saved version doesn't exist, 
        # build a new one. 

        bundle = Bundle()  
        idnt = bundle.identity
        idnt.revision = 1
        bundle.config.rewrite(identity=idnt.dict)

        bundle = Bundle()  
        marker = bundle.filesystem.build_path('test-marker')
        build_dir =  bundle.filesystem.build_path()+'/' # Slash needed for rsync
        save_dir = bundle.filesystem.build_path()+"-save/"

        if not os.path.exists(marker):
            logger.info( "Build dir marker ({}) is missing".format(marker))
            # There is a good reason to create a seperate instance, 
            # but don't remember what it is ... 

            bundle.clean()
            bundle = Bundle()   
            if not os.path.exists(save_dir):
                logger.info( "Save dir is missing; re-build bundle. ")
                bundle.prepare()
                
                bundle.build()
                
                with open(marker, 'w') as f:
                    f.write(str(time.time()))
                # Copy the newly built bundle to the save directory    
                os.system("rm -rf {1}; rsync -arv {0} {1} > /dev/null ".format(build_dir, save_dir))

        # Always copy, just to be safe. 
        logger.info(  "Copying bundle from {}".format(save_dir))
        os.system("rm -rf {0}; rsync -arv {1} {0}  > /dev/null ".format(build_dir, save_dir))


    def server_library_config(self, name='default'):

        config = self.server_rc.library(name)

        return config

    def start_server(self, config=None, name='default'):
        '''Run the Bottle server as a thread'''
        from databundles.client.siesta import  API
        import databundles.server.main
        from threading import Thread
        import time
        from functools import  partial
        from databundles.client.rest import RemoteLibrary

        config = self.server_library_config(name)

        self.server_url = "http://localhost:{}".format(config['port'])
        
        logger.info("Checking server at: {}".format(self.server_url))

        a = RemoteLibrary(self.server_url)

        #
        # Test to see of the server is already running. 
        #

        try:
            # An echo request to see if the server is running. 
            r = a.get_is_debug()
            
            if r.object:
                logger.info( 'Already running a debug server')
            else:
                logger.info( 'Already running a non-debug server')
    
            # We already have a server, so carry on
            return config
        except:
            # We'll get an exception refused eception if there is not server
            logger.info( 'No server, starting a local debug server')


        server = Thread(target = partial(databundles.server.main.test_run, config) ) 
        server.setDaemon(True)
        server.start()
        
        #databundles.server.bottle.debug()
        
        # Wait for the server to start
        for i in range(1,10): #@UnusedVariable
            try:
                # An echo request to see if the server is running. 
                r = a.get_test_echo('start_server')
                break
            except:
                logger.info( 'Server not started yet, waiting')
                time.sleep(1)
                               
        r = a.get_test_echo('start_server')
        
        return config
    
    def stop_server(self):
        '''Shutdown the server process by calling the close() API, then waiting for it
        to stop serving requests '''
        

        import socket
        import time
        import databundles.client.exceptions as exc
        from requests.exceptions import ConnectionError
        from databundles.client.rest import RemoteLibrary
        
        if not self.server_url:
            return
        
        a = RemoteLibrary(self.server_url)

        try:
            is_debug = a.get_is_debug()
        except ConnectionError:
            # Already closed:
            return
  
        if not is_debug:
            logger.info("Server is not debug, won't stop")
            return
        else:
            logger.info("Server at {} is debug, stopping".format(self.server_url))
       
        # Wait for the server to shutdown
        
        for i in range(1,10): #@UnusedVariable
            try:
                a.post_close()
                logger.info('Teardown: server still running, waiting')
                time.sleep(1)
            except socket.error:
                pass # Just means that the socket is already closed
            except IOError:
                pass # Probably just means that the socket is already closed
            except ConnectionError:
                pass # Another way the socket can be closed. Thrown by requests library.
            except Exception as e:
                logger.error("Got an exception while stopping: {} {}".format(type(e), e))
                break   
            
        time.sleep(2) # Let the socket clear
            
        
            
            
        
 