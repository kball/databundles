'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir

        
    def tearDown(self):
        pass

    def text_basic(self):
        pass
           
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())