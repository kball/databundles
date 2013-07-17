'''
Created on Jun 30, 2012

@author: eric
'''

import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  get_runconfig
from databundles.library import QueryCommand, get_library
import logging
import databundles.util

from test_base import  TestBase

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'warehouse-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'))
                                 )

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        
        print "Deleting: {}".format(self.rc.filesystem.root_dir)
        Test.rm_rf(self.rc.filesystem.root_dir)
       

          
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
        
    def get_library(self):
        """Clear out the database before the test run"""

        return get_library(self.rc, reset = True)
        
        
    def tearDown(self):
        pass

    def test_basic(self):
        
        l = self.get_library()
    
        print l.database.dsn

        db = l.database
        
        db.drop()

        db.create()
        
        r = db.install_bundle(self.bundle) #@UnusedVariable

        db.set_config_value('group', 'key','value')
        print db.get_config_value('group', 'key')

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
        l.database.clean()
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

        
    def test_install(self):
        from databundles.library import get_warehouse
        
        print "Installing"
        w = get_warehouse(self.rc)

        w.drop()
        w.create()
        
        w.install(self.bundle)
        
        for p in self.bundle.partitions:
            w.install(p)
            
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())