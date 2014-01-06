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


    def test_name(self):

        name = Name(source='source.com', 
                    dataset='dataset', 
                    subset='subset', 
                    variation='variation', 
                    type='type', 
                    part='part', 
                    version='0.0.1')

        self.assertEquals('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)

        name = name.clone()
        
        self.assertEquals('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)        


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
        self.assertEquals('source.com-dataset-subset-type-part-variation-time-space-table-grain-format-segment-0.0.1', 
                          part_name.vname)

        part_name = part_name.clone()
        
        self.assertEquals('source.com-dataset-subset-type-part-variation-time-space-table-grain-format-segment', 
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-time-space-table-grain-format-segment-0.0.1', 
                          part_name.vname)        

        partial_name = PartialName(source='source.com', dataset='dataset', type=None)

        with self.assertRaises(NotImplementedError):
            partial_name.name

        d =  partial_name.dict

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        partial_name = partial_name.clone()

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        # With a semantic version spec
        
        name = Name(source='source.com', version='0.0.1')
        self.assertEquals('source.com-orig-0.0.1',name.vname)  
        
        name.version.major = 2
        name.version.build = ('foobar',)

        self.assertEquals('source.com-orig-2.0.1+foobar',name.vname)  
        
        name = Name(source='source.com', version='>=0.0.1')

        self.assertEquals('source.com-orig>=0.0.1',name.vname)   

    def test_identity(self):

        name = Name(source='source.com', dataset='foobar',  version='0.0.1')
        on = dn = DatasetNumber(10000, 1)
        
        ident = Identity(name, on)

        self.assertEquals('d002Bi',ident.id_)   
        self.assertEquals('d002Bi001',ident.vid)   
        self.assertEquals('source.com-foobar-orig',str(ident.name))   
        self.assertEquals('source.com-foobar-orig-0.0.1',ident.vname)   
        self.assertEquals('source.com-foobar-orig-0.0.1~d002Bi001',ident.fqname)   

    def test_resolve(self):
        from  testbundle.bundle import Bundle
        
        bundle = Bundle()    
        
        bundle.identity
        
        print bundle.identity.vid
        print bundle.identity.name
        print bundle.identity.vname
        print bundle.identity.fqname

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()