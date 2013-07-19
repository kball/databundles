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