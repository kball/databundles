'''
Created on Aug 31, 2012

@author: eric
'''

import unittest
import os.path
import logging 
import databundles.util
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase
from  databundles.client.rest import RestApi #@UnresolvedImport
from databundles.library import QueryCommand, new_library
from databundles.util import rm_rf

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 
logging.captureWarnings(True)

class Test(TestBase):
 
    def setUp(self):
        
        rm_rf('/tmp/server')

        self.copy_or_build_bundle()
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        self.rc = RunConfig([os.path.join(self.bundle_dir,'client-test-config.yaml'),
                             os.path.join(self.bundle_dir,'bundle.yaml'),
                             RunConfig.USER_CONFIG])
         
        self.server_rc = RunConfig([os.path.join(self.bundle_dir,'server-test-config.yaml'),RunConfig.USER_CONFIG])
       
        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        self.stop_server()

    def web_exists(self,s3, rel_path):
    
        import requests
        import urlparse
        
        url  = s3.path(rel_path, method='HEAD')
      
        parts = list(urlparse.urlparse(url))
        qs = urlparse.parse_qs(parts[4])
        parts[4] = None
        
        url = urlparse.urlunparse(parts)
        r = requests.head(urlparse.urlunparse(parts), params=qs)

        self.assertEquals(200, r.status_code)
    
        return True

    def test_simple_install(self):
        from databundles.library import QueryCommand, new_library
        from databundles.filesystem import RestRemote, new_filesystem
        
        config = self.start_server()
        
        # Create the library so we can get the same remote config
        l = new_library(config)
        s3 = l.remote.last_upstream()

        print "Starting server with config: {}".format(config.to_dict())

        api = RestRemote(upstream=s3, **config)

        r =  api.put_bundle(self.bundle)
        print r
        self.web_exists(s3,self.bundle.identity.cache_key)


        for partition in self.bundle.partitions:
            r =  api.put_partition(partition)

            r = api.get(partition.identity.cache_key)

            self.web_exists(s3,partition.identity.cache_key )
            #os.remove(r)
  
        return 
  
        # Try variants of find. 
        r = api.find(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r[0].name)
        
        r = api.find(QueryCommand().identity(name = self.bundle.identity.name))
        self.assertEquals(self.bundle.identity.name, r[0].name)

        for partition in self.bundle.partitions:
            r = api.find((QueryCommand().partition(name = partition.identity.name)).to_dict())
            self.assertEquals(partition.identity.name, r[0].name)
  
    def x_test_remote_library(self):
   
        # This test does not work with the threaded test server. 
        
        # It does work with an external server, but you have to delete 
        # All of the files on the remote library between runs. 
   
        #
        # First store the files in the local library
        #
        
        self.start_server()
        
        self.get_library('server').purge()
        self.get_library('clean').purge()

        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)
            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)            

        #
        # Now start with a different, clean library with the same remote
        #

        # haven't pushed yet, so should fail. 
        l2 = self.get_library('clean')
        b = l2.get(self.bundle.identity.name)
        self.assertTrue(not b)
        
        # Copy all of the newly added files to the server. 
        l.push()
   
        l2 = self.get_library('clean')

        r = l2.get(self.bundle.identity.name)

        self.assertTrue(bool(r))

        r = l2.get(r.partitions.all[0].identity.id_)

        self.assertTrue(bool(r))
        self.assertTrue(os.path.exists(r.partition.database.path))
        
        
        
   
    def x_test_remote_library_partitions(self):

        self.start_server()

        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)

        # Copy all of the newly added files to the server. 
        l.push()
            
        l2 = new_library('clean')
        l2.purge()
        
        r = l2.get('b1DxuZ001')
     
        self.assertTrue(r is not None and r is not False)
        
        print r
        
        self.assertTrue(r.partition is not None and r.partition is not False)
        self.assertEquals(r.partition.identity.id_,'b1DxuZ001' )
        
        self.assertTrue(os.path.exists(r.partition.database.path))
   
    def x_test_test(self):
        from databundles.client.siesta import  API
        
        self.start_server()
        
        a = API(self.server_url)
        
        # Test echo for get. 
        r = a.test.echo('foobar').get(bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)
        
        self.assertEquals('foobar',r.object[0])
        self.assertEquals('baz',r.object[1]['bar'])
        
        # Test echo for put. 
        r = a.test.echo().put(['foobar'],bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)

        self.assertEquals('foobar',r.object[0][0])
        self.assertEquals('baz',r.object[1]['bar'])
      
        
        with self.assertRaises(Exception):
            r = a.test.exception.put('foo')
        
        with self.assertRaises(Exception):
            r = a.test.exception.get()

    def _test_put_bundle(self, name, remote_config=None):
        from databundles.bundle import DbBundle
        from databundles.library import QueryCommand
        
        rm_rf('/tmp/server')
        
        self.start_server(remote_config)
        
        r = None #Rest(self.server_url, remote_config)
        
        bf = self.bundle.database.path

        # With an FLO
        response =  r.put(open(bf), self.bundle.identity)
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))
      
        # with a path
        response =  r.put( bf, self.bundle.identity)
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))

        for p in self.bundle.partitions.all:
            response =  r.put( open(p.database.path), p.identity)
            self.assertEquals(p.identity.id_, response.object.get('id'))

        # Now get the bundles
        bundle_file = r.get(self.bundle.identity,'/tmp/foo.db')
        bundle = DbBundle(bundle_file)

        self.assertIsNot(bundle, None)
        self.assertEquals('a1DxuZ',bundle.identity.id_)

        # Should show up in datasets list. 
        
        o = r.list()
   
        self.assertTrue('a1DxuZ' in o.keys() )
    
        o = r.find(QueryCommand().table(name='tone').partition(any=True))
      
        self.assertTrue( 'b1DxuZ001' in [i.id_ for i in o])
        self.assertTrue( 'a1DxuZ' in [i.as_dataset.id_ for i in o])

    def x_test_put_bundle_noremote(self):
        return self._test_put_bundle('default')

    def x_test_put_bundle_remote(self):
        return self._test_put_bundle('default-remote', self.rc.accounts)


    def test_find_upstream(self):

        from databundles.run import  get_runconfig
        from databundles.filesystem import Filesystem, RemoteMarker

   
        def get_cache(fsname):
            rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))
            config = rc.filesystem(fsname)
            cache = Filesystem.get_cache(config)
            return cache
        
        cache = get_cache("cached-compressed-s3")
        
        print cache
        
        print cache.get_upstream(RemoteMarker)
        
            
    def test_caches(self):
        '''Basic test of put(), get() and has() for all cache types'''
        from functools import partial
        from databundles.run import  get_runconfig, RunConfig
        from databundles.filesystem import Filesystem
        from databundles.util import md5_for_file
   
        
        self.start_server() # For the rest-cache
        
        #fn = '/tmp/1mbfile'
        #with open(fn, 'wb') as f:
        #    f.write('.'*(1024))
      
        fn = self.bundle.database.path
      
        md5 = md5_for_file(fn)
        
        print "MD5 {}  = {}".format(fn, md5)

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))
        
        for i, fsname in enumerate(['fscache', 'limitedcache', 'compressioncache','cached-s3', 'cached-compressed-s3', 'rest-cache']): #'compressioncache',


            config = rc.filesystem(fsname)
            cache = Filesystem.get_cache(config)
            print '---', fsname, cache
            identity = self.bundle.identity

            relpath = identity.cache_key

            r = cache.put(fn, relpath,identity.to_meta(md5=md5))
            r = cache.get(relpath)

            if not r.startswith('http'):
                self.assertTrue(os.path.exists(r), str(cache))
                
            self.assertTrue(cache.has(relpath, md5=md5))
            
            cache.remove(relpath, propagate=True)
            
            self.assertFalse(os.path.exists(r), str(cache))
            self.assertFalse(cache.has(relpath))
            

        cache = Filesystem.get_cache(rc.filesystem('s3cache-noupstream'))         
        r = cache.put(fn, 'a')
 
            
            
    def x_test_remote_cache(self):
        self.start_server(name='default-remote')
    
    def x_test_put_redirect(self):
        from databundles.bundle import DbBundle
        from databundles.library import QueryCommand
        from databundles.util import md5_for_file, rm_rf, bundle_file_type

        #
        # Simple out and retrieve
        # 
        cache = self.bundle.filesystem._get_cache(self.server_rc.filesystem, 'direct-remote')
        cache2 = self.bundle.filesystem._get_cache(self.server_rc.filesystem, 'direct-remote-2')

        rm_rf(os.path.dirname(cache.cache_dir))
        rm_rf(os.path.dirname(cache2.cache_dir))
        
        cache.put( self.bundle.database.path, 'direct')

        path = cache2.get('direct')

        self.assertEquals('sqlite',bundle_file_type(path))

        cache.remove('direct', propagate = True)

        #
        #  Connect through server. 
        #
        rm_rf('/tmp/server')
        self.start_server(name='default-remote')
        
        api = None # Rest(self.server_url, self.rc.accounts)  

        # Upload directly, then download via the cache. 
        
        cache.remove(self.bundle.identity.cache_key, propagate = True)
        
        r = api.upload_file(self.bundle.identity, self.bundle.database.path, force=True )

        path = cache.get(self.bundle.identity.cache_key)
        
        b = DbBundle(path)

        self.assertEquals("source-dataset-subset-variation-ca0d",b.identity.name )
      
        #
        # Full service
        #

        p  = self.bundle.partitions.all[0]

        cache.remove(self.bundle.identity.cache_key, propagate = True)
        cache.remove(p.identity.cache_key, propagate = True)
        
        r = api.put( self.bundle.database.path, self.bundle.identity )
        print "Put {}".format(r.object)
        r = api.put(p.database.path, p.identity )
        print "Put {}".format(r.object)
        
        r = api.put(p.database.path, p.identity )
        
        r = api.get(p.identity,'/tmp/foo.db')
        print "Get {}".format(r)        

        b = DbBundle(r)

        self.assertEquals("source-dataset-subset-variation-ca0d",b.identity.name )

        
    def x_test_dump(self):
        import time
        import logging 
     
       
        l = new_library(self.server_rc.library('default-remote'), reset = True)
        l.clean()

        self.start_server()
        
        l.run_dumper_thread()
        l.run_dumper_thread()
       
        self.assertFalse(l.database.needs_dump())
        l.put(self.bundle)
        self.assertTrue(l.database.needs_dump()) 
        l.run_dumper_thread()
        time.sleep(6)
        self.assertFalse(l.database.needs_dump())
            
        l.run_dumper_thread()
        l.put(self.bundle)
        l.run_dumper_thread()
        time.sleep(7)
        print l.database.needs_dump()
        self.assertFalse(l.database.needs_dump())
        
        self.assertEquals(self.bundle.identity.name,  l.get(self.bundle.identity.name).identity.name)
        
        l.clean()
        
        self.assertEqual(None, l.get(self.bundle.identity.name))
        
        l.restore()
        
        self.assertEquals(self.bundle.identity.name,  l.get(self.bundle.identity.name).identity.name)
        
        
        
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())