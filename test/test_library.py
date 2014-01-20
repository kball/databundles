'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  get_runconfig, RunConfig
from databundles.library.query import QueryCommand
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
        from databundles.library import new_library

        config = self.rc.library(name)

        l =  new_library(config, reset = True)

        return l
        
        
        
    def tearDown(self):
        pass

    @staticmethod
    def new_db():
        from databundles.util import temp_file_name
        from databundles.library.database import LibraryDb
        db_file = temp_file_name()+".db"

        db = LibraryDb(driver='sqlite', dbname=db_file)

        return db_file, db

    def test_database(self):

        f,db = self.new_db()

        ##
        ## Test basic creation
        ##

        self.assertFalse(db.exists())

        db.create()

        self.assertTrue(db.exists())

        db.set_config_value('test','one',1)
        db.set_config_value('test','two',2)

        self.assertEquals(1,db.get_config_value('test','one').value)
        self.assertEquals(2,db.get_config_value('test','two').value)

        self.assertIn(('test', 'one'),db.config_values)
        self.assertIn(('test', 'two'),db.config_values)
        self.assertEquals(2,db.config_values[('test', 'two')])

        self.assertEquals(0, len(db.list()))

        db.drop()

        self.assertTrue(os.path.exists(f))
        self.assertFalse(db.exists())

        os.remove(f)

    def test_database_query(self):
        from databundles.orm import Dataset, Partition
        from databundles.library.query import Resolver
        from databundles.library.database import ROOT_CONFIG_NAME_V

        f,db = self.new_db()
        print 'Testing ', f
        db.create()

        db.install_bundle(self.bundle)

        #
        # Get a bunch of names from the existing bundles. This will check the simple
        # queries for single objects.
        #

        tests = {}
        for r in db.session.query(Dataset, Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all():

            di = r.Dataset.identity

            tests[di.sname] = di.vid
            tests[di.vname] = di.vid
            tests[di.fqname] = di.vid
            tests[di.vid] = di.vid

            pi = r.Partition.identity

            tests[pi.sname] = pi.vid
            tests[pi.vname] = pi.vid
            tests[pi.fqname] = pi.vid
            tests[pi.vid] = pi.vid

        r = Resolver(db.session)


        for ref, vid in tests.items():
            ip, results = r.resolve_ref_all(ref)

            self.assertEqual(1, len(results))

            first= results.values().pop(0)
            vid2 = first.vid if not first.partitions  else first.partitions.values()[0].vid

            self.assertEquals(vid, vid2)


    def test_simple_install(self):

        from databundles.util import temp_file_name
        import pprint
        import os
        
        l = self.get_library()
        print "Library: ", l.database.dsn
     
        r = l.put(self.bundle) #@UnusedVariable

        r = l.get(self.bundle.identity.sname)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        r = l.get('dibberish')
        self.assertFalse(r)

        for partition in self.bundle.partitions:
            print "Install and check: ", partition.identity.vname
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.sname)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.sname, r.partition.identity.sname)
            self.assertEquals(self.bundle.identity.sname, r.identity.sname)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)

            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.sname, r.partition.identity.sname)
            self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        self.assertTrue(l.database.needs_dump())

        backup_file = temp_file_name()+".db"
        
        l.database.dump(backup_file)
        
        l.database.close()
        os.remove(l.database.dbname)
        l.database.create()

        r = l.get(self.bundle.identity.sname)
    
        self.assertTrue(not r)
        
        l.database.restore(backup_file)
        
        r = l.get(self.bundle.identity.sname)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        os.remove(backup_file)

        # An extra change so the following tests work
        l.put(self.bundle)
        
        self.assertFalse(l.database.needs_dump())

        import time; time.sleep(10)

        self.assertTrue(l.database.needs_dump())
      
      


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

            print type(partition.identity), partition.identity

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

        self.assertEquals('source-dataset-subset-variation',r[0]['identity']['name'])
    
        r = l.find(QueryCommand().table(name='tone').partition(format='db', grain=None))

        self.assertEquals('source-dataset-subset-variation-tone',r[0]['partition']['name'])
        
        r = l.find(QueryCommand().table(name='tthree').partition(format='db', segment=None))
        self.assertEquals('source-dataset-subset-variation-tthree',r[0]['partition']['name'])

        r = l.find(QueryCommand().table(name='tthree').partition(format='db', segment="1"))
        self.assertEquals('source-dataset-subset-variation-tthree-1',r[0]['partition']['name'])
        
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
        self.assertEquals(2, len(r))
       
        ds_names = [ds.sname for ds in l.list()]
        self.assertIn('source-dataset-subset-variation', ds_names)

    def test_versions(self):
        import testbundle.bundle
        from databundles.run import get_runconfig
        from databundles.library.query import Resolver
        import shutil
        idnt = self.bundle.identity
       
        f,db = self.new_db()
        print 'Testing ', f
        db.create()

        orig = os.path.join(self.bundle.bundle_dir,'bundle.yaml')
        save = os.path.join(self.bundle.bundle_dir,'bundle.yaml.save')
        shutil.copyfile(orig,save)

        datasets = {}

        try:
            for i in [1,2,3]:
                idnt._on.revision = i
                idnt.name.version_major = i
                idnt.name.version_minor = i*10

                bundle = Bundle()
                bundle.config.rewrite(identity=idnt.ident_dict,
                                      names=idnt.names_dict)
                get_runconfig.clear() #clear runconfig cache

                print 'Building version {}'.format(i)

                bundle = Bundle()

                bundle.clean()
                bundle.pre_prepare()
                bundle.prepare()
                bundle.post_prepare()
                bundle.pre_build()
                bundle.build_small()
                #bundle.build()
                bundle.post_build()

                bundle = Bundle()

                print "Installing ", bundle.identity.vname
                db.install_bundle(bundle)

        finally:
            pass
            os.rename(save, orig)

        #
        # Save the list of datasets for version analysis in other
        # tests
        #

        for d in db.list().values():
            datasets[d.vid] = d.dict
            datasets[d.vid]['partitions'] = {}

            for p_vid, p in d.partitions.items():
                datasets[d.vid]['partitions'][p_vid] = p.dict


        with open(self.bundle.filesystem.path('meta','version_datasets.json'),'w') as f:
            import json
            f.write(json.dumps(datasets))


        r = Resolver(db.session)

        ref = idnt.id_

        ref = "source-dataset-subset-variation-=2.20"

        ip, results = r.resolve_ref_all(ref)

        for row in results:
            print row


        #os.remove(f)


    def test_version_resolver(self):
        from databundles.library.query import Resolver

        l = self.bundle.library


        db = l.database
        db.enable_delete = True
        db.drop()
        db.create()

        l.put_bundle(self.bundle)

        #for _, ident in db.list().items():
        #    print '--', ident.fqname
        #    for _, p_ident in ident.partitions.items():
        #        print '  ', p_ident.fqname


        r = Resolver(db.session)

        vname = 'source-dataset-subset-variation-0.0.1'
        name = 'source-dataset-subset-variation'

        ip, results = r.resolve_ref_one(vname)
        self.assertEquals(vname, results.vname)

        ip, results = r.resolve_ref_one(name)
        self.assertEquals(vname, results.vname)

        # Cache keys

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1.db')
        self.assertEquals('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1/tthree.db')
        self.assertEquals('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8001001',str(result.partition))


        # Now in the library, which has a slightly different interface.

        ident = l.resolve(vname)
        self.assertEquals(vname, ident.vname)

        ident = l.resolve('source-dataset-subset-variation-0.0.1~diEGPXmDC8001')
        self.assertEquals('diEGPXmDC8001', ident.vid)

        ident = l.resolve('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8001001')
        self.assertEquals('piEGPXmDC8001001', ident.vid)

        ##
        ## Test semantic version matching
        ## WARNING! The Mock object below only works for testing semantic versions.
        ##

        with open(self.bundle.filesystem.path('meta','version_datasets.json')) as f:
            import json
            datasets = json.loads(f.read())

        # This mock object only works on datasets; it will return all of the
        # partitions for each dataset, and each of the datasets. It is only for testing
        # version filtering.
        class TestResolver(Resolver):
            def resolve_ref_all(self, ref):
                from databundles.identity import Identity
                ip = Identity.classify(ref)
                return ip, { k:Identity.from_dict(ds) for k,ds in datasets.items() }

        r = TestResolver(db.session)


        ip, result = r.resolve_ref_one('source-dataset-subset-variation-==1.10.1')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<2.0.0')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->2.0.0')
        self.assertEquals('source-dataset-subset-variation-3.30.3~diEGPXmDC8003',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation-<=3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002',str(result))




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

        print library.upstream
        print library.upstream.last_upstream()
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
        from databundles.identity import PartitionNameQuery
        from sqlalchemy.exc import IntegrityError
        
        l = self.get_library()
        
        l.purge()
         
        #
        # Create all possible combinations of partition names
        # 
        s = set()
        table = self.bundle.schema.tables[0]

        p = (('time','time2'),('space','space3'),('grain','grain4'))
        p += p
        pids = {}
        for i in range(4):
            for j in range(4):
                pid = self.bundle.identity.as_partition(**dict(p[i:i+j+1]))
                pids[pid.fqname] = pid

        for pid in pids.values():
            print pid.sname
            try:
                # One will fail with an integrity eorror, but it doesn't matter for this test.

                part = self.bundle.partitions.new_db_partition(**pid.dict)
                part.create()
                
                parts = self.bundle.partitions._find_orm(PartitionNameQuery(vid=pid.vid)).all()
                self.assertIn(pid.sname, [p.name for p in parts])
            except IntegrityError: 
                pass
    
    
        l.put(self.bundle) # Install the partition references in the library. 

        b = l.get(self.bundle.identity)

        for partition in self.bundle.partitions:

            l.put(partition)
            l.put(partition)

            print partition.identity.sname

            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)

        hdf = l.get('source-dataset-subset-variation-hdf5-hdf')
        
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
        from databundles.library.query import QueryCommand
        
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