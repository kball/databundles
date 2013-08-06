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
        """Setup a library that will download bundles from production, freshly for every run"""
        import testbundle.bundle
        from databundles.util import rm_rf
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'client-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'))
                                 )

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        #print "Deleting: {}".format(self.rc.filesystem.root_dir)
        #rm_rf(self.rc.filesystem.root_dir)
       

        self.library =   get_library(self.rc, 'production', reset = True)

    def test_basic(self):
    
        b = self.library.get('clarinova.com-places-casnd-7ba4.places')
        
        print b.database.path
   
        for table in b.schema.tables:
            print table
            for col in table.columns:
                print '    ', col.name, col.foreign_key

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())