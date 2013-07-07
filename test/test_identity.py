'''
Created on Jul 6, 2013

@author: eric
'''
import unittest
from databundles.identity import *

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_basic(self):
        dnn = 1000000
        rev = 100
        
        
        dn = DatasetNumber(dnn)
        self.assertEquals('a4c92', str(dn))
        
        dn = DatasetNumber(dnn, rev)
        self.assertEquals('a4c9201C', str(dn))

        self.assertEquals('a4c9201C', str(ObjectNumber.parse(str(dn), True)))

        tn = TableNumber(dn, 1)

        self.assertEquals('c4c920101C', str(tn))
        
        self.assertEquals('c4c920101C', str(ObjectNumber.parse(str(tn), True)))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()