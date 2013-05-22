'''
Created on Jan 17, 2013

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.identity import * #@UnusedWildImport
from test_base import  TestBase

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir

    def test_basic(self):

        for p in self.bundle.partitions:
            print type(p.identity),  p.identity.name
            
            
        p = self.bundle.partitions.find_or_new_geo(table='geot1', space='all')
        p.create()
        print p.database.path
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()