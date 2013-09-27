'''
Created on Jun 30, 2012

@author: eric
'''

import unittest
import os.path
from test_base import  TestBase
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  get_runconfig
from databundles.run import  RunConfig

from source.repository import new_repository

import logging
import databundles.util


logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'source-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_CONFIG))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root_dir)
        databundles.util.rm_rf(self.rc.group('filesystem').root_dir)


    def tearDown(self):
        pass

 
    def testBasic(self):
        import random
        
        repo = new_repository(self.rc.sourcerepo('clarinova.data'))
    
        print repo
        
    
        name = 'foobar'; # %08x' % random.randrange(8**30)
        
        if not repo.service.has(name):
       
            print "Creating repo ", name
            
            repo.service.create(name)
        
            
        repo.init()
 
        repo.add('bundle.py')
        
        repo.commit()
 

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())