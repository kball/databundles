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
        
        from databundles.partition import  new_identity
    
        idnt = Identity(source='source.com', dataset='dataset', 
                         subset='subset', variation='variation', 
                         revision=1, creator='xxx')
       
        part = new_identity(dict(table='table',grain='grain', **(idnt.to_dict())))

        print part.name
        print part.vname
        print part.path
        print part.cache_key

        names = [
                 idnt.name, 
                 idnt.vname, 
                 part.name, 
                 part.vname,
                 'census.gov-geography-dim-orig-a7d9.polygons.ca.nonblocks',
                 'census.gov-geography-dim-orig-a7d9-r1.polygons.ca.nonblocks',
                 'census.gov-geography-dim-orig-a7d9.polygons.ca',
                 'census.gov-geography-dim-orig-a7d9.polygons',
                 'census.gov-geography-dim-orig-a7d9-r1',
                 'census.gov-geography-orig-a7d9.polygons.ca.nonblocks',
                 'census.gov-geography-orig-a7d9-r1.polygons.ca.nonblocks',
                 'census.gov-geography-orig-a7d9.polygons.ca',
                 'census.gov-geography-orig-a7d9.polygons',
                 'census.gov-geography-orig-a7d9-r1'
                 ]

        for name in names:
            print "{:70s} {}".format(name, Identity.parse_name(name).name)

        names = [

                 'census.gov-geography-dim-orig-a7d9-polygons.ca.nonblocks',
                 'census.gov-orig-a7d9-r1.polygons.ca.nonblocks',
                 'census.gov-geog.raphy-dim-orig-a7d9.polygons.ca',
               
                 ]

        for name in names:
            try:
                print "{:70s} {}".format(name, Identity.parse_name(name).name)
            except Exception as e:
                print e.message



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