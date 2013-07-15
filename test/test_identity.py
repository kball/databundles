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


    def test_name(self):

    
        idnt = Identity(source='source.com', dataset='dataset', 
                         subset='subset', variation='variation', 
                         revision=1, creator='xxx')
       
        part = PartitionIdentity(idnt, table='table',grain='grain')


        print idnt.name, Identity.parse_name(idnt.name).vname
        print idnt.vname, Identity.parse_name(idnt.vname).vname
        print part.name, Identity.parse_name(part.name).vname
        print part.vname, Identity.parse_name(part.vname).vname
        
        print Identity.parse_name('foobar')

    def test_id(self):
        dnn = 1000000
        rev = 100
        
        dn = DatasetNumber(dnn)
        self.assertEquals('a4c92', str(dn))
        
        dn = DatasetNumber(dnn, rev)
        self.assertEquals('a4c92/01C', str(dn))

        self.assertEquals('a4c92/01C', str(ObjectNumber.parse(str(dn))))

        tn = TableNumber(dn, 1)

        self.assertEquals('c4c9201/01C', str(tn))
        
        self.assertEquals('c4c9201/01C', str(ObjectNumber.parse(str(tn))))

        tnnr = tn.rev(None)
        
        self.assertEquals('c4c9201', str(tnnr))

        self.assertEquals('c4c9201/004', str(tnnr.rev(4)))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()