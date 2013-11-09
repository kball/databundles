'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase
import os
from databundles.run import  RunConfig

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        self.rc = RunConfig([os.path.join(self.bundle_dir,'client-test-config.yaml'),
                             os.path.join(self.bundle_dir,'bundle.yaml'),
                             RunConfig.USER_CONFIG])
         
        self.server_rc = RunConfig([os.path.join(self.bundle_dir,'server-test-config.yaml'),RunConfig.USER_CONFIG])
       
        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir
        
    def tearDown(self):
        pass

           
    def test_caches(self):
        '''Basic test of put(), get() and has() for all cache types'''
        from databundles.run import  get_runconfig
        from databundles.cache import new_cache
        from databundles.util import md5_for_file
        from databundles.bundle import DbBundle
        
        self.start_server() # For the rest-cache
        
        #fn = '/tmp/1mbfile'
        #with open(fn, 'wb') as f:
        #    f.write('.'*(1024))
      
        fn = self.bundle.database.path
      
        # Opening the file might run the database updates in 
        # database.sqlite._on_connect_update_schema, which can affect the md5.
        b = DbBundle(fn)
      
        md5 = md5_for_file(fn)
    
        
        print "MD5 {}  = {}".format(fn, md5)

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))
        
        for i, fsname in enumerate(['fscache', 'limitedcache', 'compressioncache','cached-s3', 'cached-compressed-s3']): #'compressioncache',

            config = rc.filesystem(fsname)
            cache = new_cache(config)
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
            

        cache = new_cache(rc.filesystem('s3cache-noupstream'))         
        r = cache.put(fn, 'a')
 
          
    def test_compression(self):
        from databundles.run import  get_runconfig
        from databundles.cache import new_cache
        from databundles.util import  temp_file_name, md5_for_file, StreamingGZip, copy_file_or_flo
        
        fn =  temp_file_name()
        print 'Write to ', fn
        with open(fn,'wb') as f:
            for i in range(1000):
                f.write("{:03d}\n".format(i))

        md5_orig = md5_for_file(fn)

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))

        comp_cache = new_cache(rc.filesystem('compressioncache'))
        
        cf = comp_cache.put(fn, 'compressed')

        md5_comp = md5_for_file(cf)
        
        print cf, md5_orig, md5_comp
        
        with open(cf) as stream:
            stream = StreamingGZip(fileobj=stream)
            
            uncomp_cache = new_cache(rc.filesystem('fscache'))
            
            uncomp_stream = uncomp_cache.put_stream('decomp')
            
            copy_file_or_flo(stream, uncomp_stream)
            
        dcf = uncomp_cache.get('decomp')
        
        
        print md5_for_file(fn), fn
        print  md5_for_file(dcf), dcf
       
         
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())