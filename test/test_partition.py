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

    def x_test_basic(self):

        for p in self.bundle.partitions:
            print type(p.identity),  p.identity.name
            
            
        p = self.bundle.partitions.find_or_new_geo(table='geot1', space='all')
        p.create()
        print p.database.path
        
    def test_segments(self):
        
        from databundles.partition import PartitionIdentity
        from databundles.identity import Identity
        
        names = []
        for i in range(1,10):
            pid = PartitionIdentity(self.bundle.identity, space='city', segment = i)

            p = self.bundle.partitions.new_db_partition(pid)

            names.append(p.identity.name)

        for p in self.bundle.partitions.find_all(space='city', segment=Identity.ANY):
            self.assertIn(p.identity.name, names)


        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()