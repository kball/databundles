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


    def tearDown(self):
        pass

    def test_basic(self):
        from databundles.datasets.geo import US
        
        us = US(self.bundle.library)

        for state in us.states:
            print state.row, state.name, state.fips
            
        self.assertEquals(51, us.state('VA').fips)
        self.assertEquals(1779803, us.state('VA').ansi)        
        
        self.assertEquals('WY', us.state(fips=56).usps)
        self.assertEquals('WI', us.state(fips=55).usps)        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()