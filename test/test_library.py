'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  get_runconfig, RunConfig
from databundles.library import QueryCommand, new_library
import logging
import databundles.util

from test_base import  TestBase



logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'library-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_CONFIG)
                                 )

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root_dir)
        Test.rm_rf(self.rc.group('filesystem').root_dir)
       
    @staticmethod
    def rm_rf(d):
        
        if not os.path.exists(d):
            return
        
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                Test.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)
        
    def get_library(self, name = 'default'):
        """Clear out the database before the test run"""

        config = self.rc.library(name)

        l =  new_library(config, reset = True)

        return l
        
        
        
    def tearDown(self):
        pass

    def test_simple_install(self):
        from databundles.library import QueryCommand
        from databundles.util import temp_file_name
        import pprint
        import os
        
        l = self.get_library()
        print "Library: ", l.database.dsn
     
        r = l.put(self.bundle) #@UnusedVariable

        r = l.get(self.bundle.identity.name)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        r = l.get('gibberish')
        self.assertFalse(r)

        for partition in self.bundle.partitions:
            print "Install and check: ", partition.identity.vname
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)

            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)            

        self.assertTrue(l.database.needs_dump())

        backup_file = temp_file_name()+".db"
        
        l.database.dump(backup_file)
        
        l.database.close()
        os.remove(l.database.dbname)
        l.database.create()

        r = l.get(self.bundle.identity.name)
    
        self.assertTrue(not r)
        
        l.database.restore(backup_file)
        
        r = l.get(self.bundle.identity.name)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        os.remove(backup_file)

        # An extra change so the following tests work
        l.put(self.bundle)
        
        self.assertFalse(l.database.needs_dump())

        import time; time.sleep(10)

        self.assertTrue(l.database.needs_dump())
      
      
    def test_resolve(self):
        """Test the resolve_id() function"""
        from databundles import resolve_id
        
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle) )
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle.identity))
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle.identity.id_))
        self.assertEquals(self.bundle.identity.id_, resolve_id(str(self.bundle.identity.id_)))

        for partition in self.bundle.partitions.all:
            self.assertEquals(partition.identity.id_, resolve_id(partition))
            self.assertEquals(partition.identity.id_, resolve_id(partition.identity))
            self.assertEquals(partition.identity.id_, resolve_id(partition.identity.id_))
            self.assertEquals(partition.identity.id_, resolve_id(str(partition.identity.id_)))            
            
    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
      
        l = self.get_library()
     
        print l.info
     
        l.put(self.bundle)
        l.put(self.bundle)
 
        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r)
        self.assertTrue(r is not False)
        self.assertEquals(r.identity.id_, r.identity.id_)
        
        print "Stored: ",  r.identity.name
        
        # Install the partition, then check that we can fetch it
        # a few different ways. 
        for partition in self.bundle.partitions:
            l.put(partition)
            l.put(partition)
            
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)
            
        # Re-install the bundle, then check that the partitions are still properly installed

        l.put(self.bundle)
        
        for partition in self.bundle.partitions.all:
       
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
        # Find the bundle and partitions in the library. 
    
        r = l.find(QueryCommand().table(name='tone'))

        self.assertEquals('source-dataset-subset-variation-ca0d',r[0]['identity']['name'])  
    
        r = l.find(QueryCommand().table(name='tone').partition(format='db', grain=None))

        self.assertEquals('source-dataset-subset-variation-ca0d.tone',r[0]['partition']['name'])
        
        r = l.find(QueryCommand().table(name='tthree').partition(format='db', segment=None))
        self.assertEquals('source-dataset-subset-variation-ca0d.tthree',r[0]['partition']['name'])

        r = l.find(QueryCommand().table(name='tthree').partition(format='db', segment="1"))
        self.assertEquals('source-dataset-subset-variation-ca0d.tthree.1',r[0]['partition']['name'])
        
        #
        #  Try getting the files 
        # 
        
        r = l.find(QueryCommand().table(name='tthree').partition(any=True)) #@UnusedVariable
       
        bp = l.get(r[0]['identity']['id'])
        
        self.assertTrue(os.path.exists(bp.database.path))
        
        # Put the bundle with remove to check that the partitions are reset
        
        l.remove(self.bundle)
        
        r = l.find(QueryCommand().table(name='tone').partition(any=True))
        self.assertEquals(0, len(r))      
        
        l.put(self.bundle)
    
        r = l.find(QueryCommand().table(name='tone').partition(any=True))
        self.assertEquals(12, len(r))
       
        ds_names = [ds.identity.name for ds in l.datasets]
        self.assertIn('source-dataset-subset-variation-ca0d', ds_names)


    def test_versions(self):
        import testbundle.bundle
        from databundles.run import get_runconfig
        idnt = self.bundle.identity
       
        #Test.rm_rf(self.bundle.filesystem.build_path())

        l = self.get_library()
        print "Database: ", l.database.dsn

        vnames = {}
        name = None
        
        for i in [1,2,3]:
            idnt.revision = i

            bundle = Bundle()  
            bundle.config.rewrite(identity=idnt.to_dict())
            get_runconfig.clear()
           
            print 'Building version {}'.format(i)

            bundle = Bundle() 
             
            bundle.clean()
            bundle.prepare()
            bundle.build()
            bundle.update_configuration()

            print "Installing ", bundle.identity.vname
            l = self.get_library()
            r = l.put(bundle)
        
            name = bundle.identity.name # Same every time. 
            vnames[i] = bundle.identity.vname
        
        
        r = l.get(name)
        
        for i,vname in vnames.items():
            r = l.get(vname)
            print i, vname, r, r.identity.vname

        
        idnt.revision = 1
        self.bundle.config.rewrite(identity=idnt.to_dict())


    def test_cache(self):
        
        from databundles.cache.filesystem import  FsCache, FsLimitedCache
     
        root = self.rc.group('filesystem').root_dir
      
        l1_repo_dir = os.path.join(root,'repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'repo-l2')
        os.makedirs(l2_repo_dir)
        
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024):
                f.write('.'*1023)
                f.write('\n')
        
        #
        # Basic operations on a cache with no upstream
        #
        l2 =  FsCache(l2_repo_dir)

        p = l2.put(testfile,'tf1')
        l2.put(testfile,'tf2')
        g = l2.get('tf1')
                
        self.assertTrue(os.path.exists(p))  
        self.assertTrue(os.path.exists(g))
        self.assertEqual(p,g)

        self.assertIsNone(l2.get('foobar'))

        l2.remove('tf1')
        
        self.assertIsNone(l2.get('tf1'))
       
        #
        # Now create the cache with an upstream, the first
        # cache we created
       
        l1 =  FsLimitedCache(l1_repo_dir, upstream=l2, size=5)
      
        print l1
        print l2
      
        g = l1.get('tf2')
        self.assertTrue(g is not None)
     
        # Put to one and check in the other. 
        
        l1.put(testfile,'write-through')
        self.assertIsNotNone(l2.get('write-through'))
             
        l1.remove('write-through', propagate=True)
        self.assertIsNone(l2.get('write-through'))

        # Put a bunch of files in, and check that
        # l2 gets all of the files, but the size of l1 says constrained
        for i in range(0,10):
            l1.put(testfile,'many'+str(i))
            
        self.assertEquals(4194304, l1.size)


        # Check that the right files got deleted
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many5')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        
        # Fetch a file that was displaced, to check that it gets loaded back 
        # into the cache. 
        p = l1.get('many1')
        p = l1.get('many2')
        self.assertTrue(p is not None)
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many1')))  
        # Should have deleted many6
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many7')))
        
        #
        # Check that verification works
        # 
        l1.verify()

        os.remove(os.path.join(l1.cache_dir, 'many8'))
            
        with self.assertRaises(Exception):                
            l1.verify()

        l1.remove('many8')
      
        l1.verify()
        
        c = l1.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", ('many9',) )
        l1.database.commit()
        
        with self.assertRaises(Exception):        
            l1.verify()
        
        l1.remove('many9')
      
        l1.verify()

    def x_test_remote(self):
        from databundles.run import RunConfig
        from databundles.library import new_library
        
        rc = get_runconfig((os.path.join(self.bundle_dir,'server-test-config.yaml'),RunConfig.USER_CONFIG))

        config = rc.library('default')
        library =  new_library(config)

        print library.remote
        print library.remote.last_upstream()
        print library.cache
        print library.cache.last_upstream()  
                                           
    def test_compression_cache(self):
        '''Test a two-level cache where the upstream compresses files '''
        from databundles.cache.filesystem import  FsCache,FsCompressionCache
         
        root = self.rc.group('filesystem').root_dir
      
        l1_repo_dir = os.path.join(root,'comp-repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'comp-repo-l2')
        os.makedirs(l2_repo_dir)
        
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024): #@UnusedVariable
                f.write('.'*1023)
                f.write('\n')

        # Create a cache with an upstream wrapped in compression
        l3 = FsCache(l2_repo_dir)
        l2 = FsCompressionCache(l3)
        l1 = FsCache(l1_repo_dir, upstream=l2)
      
        f1 = l1.put(testfile,'tf1')         
  
        self.assertTrue(os.path.exists(f1))  
        
        l1.remove('tf1', propagate=False)
        
        self.assertFalse(os.path.exists(f1))  
        
        f1 = l1.get('tf1')
        
        self.assertIsNotNone(f1)
        
        self.assertTrue(os.path.exists(f1))  
        

    def test_partitions(self):
        from databundles.partition import PartitionIdentity
        from sqlalchemy.exc import IntegrityError
        
        l = self.get_library()
        
        l.purge()
         
        #
        # Create all possible combinations of partition names
        # 
        s = set()
        table = self.bundle.schema.tables[0]
        
        p = (('time',1),('space',2),('table',table.name),('grain',4))
        p += p
        pids = []
        for i in range(4):
            for j in range(4):
                s.add(p[i:i+j+1])
            
        for v in s:
            pid = PartitionIdentity(self.bundle.identity,**dict(v))
            pids.append(pid)
        
        for pid in pids:
            try:
                # One will fail with an integrity eorror, but it doesn't matter for this test. 
                part = self.bundle.partitions.new_db_partition(pid)
                part.create()
                
                parts = self.bundle.partitions._find_orm(pid).all()
                self.assertIn(pid.name, [p.name for p in parts])
            except IntegrityError: 
                pass
    
    
        l.put(self.bundle) # Install the partition references in the library. 
    
        for partition in self.bundle.partitions:
            
            print "Install {}".format(partition.identity.name)
            l.put(partition)
            l.put(partition)
            
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)
        
        
        hdf = l.get('source-dataset-subset-variation-ca0d.hdf5')
        
        print hdf.database.path
        print hdf.partition.database.path
        
    def test_s3(self):

        #databundles.util.get_logger('databundles.filesystem').setLevel(logging.DEBUG) 
        # Set up the test directory and make some test files. 
        from databundles.cache import new_cache
        
        root = self.rc.group('filesystem').root_dir
        os.makedirs(root)
                
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024):
                f.write('.'*1023)
                f.write('\n')
         
        #fs = self.bundle.filesystem
        #local = fs.get_cache('downloads')
        
        cache = new_cache(self.rc.filesystem('s3'))
        repo_dir  = cache.cache_dir
      
        print "Repo Dir: {}".format(repo_dir)
      
        for i in range(0,10):
            logger.info("Putting "+str(i))
            cache.put(testfile,'many'+str(i))
        
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
                
        p = cache.get('many1')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
        
        p = cache.get('many2')
        self.assertTrue(p is not None)
                
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many7'))) 
 
        p = cache.get('many3')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many7'))) 
 
    def test_query(self):
        from databundles.library import QueryCommand
        
        tests = [
            "column.name = 'column.name', identity.id='identity',",
            "column.name = 'column.name', identity.id='identity' ",
            "column.name = 'column.name' identity.id = 'identity'",
            "partition.vname ='partition.vname'",
            "partition.vname = '%partition.vname%'",
            "identity.name = '%clarinova foo bar%'"
            
            ]
        
        fails = [
            "column.name='foobar"
        ]
        
        for s in tests:
            qc = QueryCommand.parse(s)
            print qc
    
        for s in fails:

            self.assertRaises(QueryCommand.ParseError, QueryCommand.parse, s)
       
       
       

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())