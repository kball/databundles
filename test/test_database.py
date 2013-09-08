'''
Created on Jun 30, 2012

@author: eric
'''

import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  get_runconfig
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
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'database-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml')))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

    def tearDown(self):
        pass

    def test_create(self):
        from databundles.database import new_database
        
        for k in ['sqlite','sqlite-warehouse']:
            d = new_database(self.rc.database(k), bundle=self.bundle)
         
            d.delete()
            d.create()
            
            print d.dsn, d.tables()

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())