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
        
        from databundles.identity import  new_identity
    
        name = Name(source='source.com', 
                    dataset='dataset', 
                    subset='subset', 
                    variation='variation', 
                    type='type', 
                    part='part', 
                    version='0.0.1')

        self.assertEquals('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEquals('source.com-dataset-subset-type-part-variation=0.0.1', name.vname)

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment='segment',
                                  **name.dict
                                  )

        self.assertEquals('source.com-dataset-subset-type-part-variation-time-space-table-grain-format-segment', 
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-time-space-table-grain-format-segment=0.0.1', 
                          part_name.vname)

        partial_name = PartialName(source='source.com', dataset='dataset', type=None)

        with self.assertRaises(NotImplementedError):
            partial_name.name

        d =  partial_name.dict

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

    def x_test_something_else(self):

        dnn = 1000000
        rev = 100
        
        dn = DatasetNumber(dnn, rev)
        
        idnt = Identity(**dict(name.dict.items()+[('vid',str(dn))]))

        part = new_identity(dict(table='table',grain='grain', **idnt.dict))

        print part.dict
        print part.sname

        return

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
        self.assertEquals('d04c92', str(dn))
        
        dn = DatasetNumber(dnn, rev)
        self.assertEquals('d04c9201C', str(dn))

        self.assertEquals('d04c9201C', str(ObjectNumber.parse(str(dn))))

        tn = TableNumber(dn, 1)

        self.assertEquals('t04c920101C', str(tn))

        self.assertEquals('t04c920101C', str(ObjectNumber.parse(str(tn))))

        tnnr = tn.rev(None)
        
        self.assertEquals('t04c9201', str(tnnr))

        self.assertEquals('t04c9201004', str(tnnr.rev(4)))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()