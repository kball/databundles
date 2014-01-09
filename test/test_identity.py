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
        self.assertEquals('d000004c92', str(dn))
        
        dn = DatasetNumber(dnn, rev)
        self.assertEquals('d000004c9201C', str(dn))

        self.assertEquals('d000004c9201C', str(ObjectNumber.parse(str(dn))))

        tn = TableNumber(dn, 1)

        self.assertEquals('t000004c920101C', str(tn))

        self.assertEquals('t000004c920101C', str(ObjectNumber.parse(str(tn))))

        tnnr = tn.rev(None)
        
        self.assertEquals('t000004c9201', str(tnnr))

        self.assertEquals('t000004c9201004', str(tnnr.rev(4)))


        # Other assignment classes
        
        dnn = 62*62+11
        
        dn = DatasetNumber(62**3-1,None,'authoritative')
        self.assertEquals('dZZZ', str(dn))
        
        dn = DatasetNumber(62**3-1,None,'registered')
        self.assertEquals('d00ZZZ', str(dn))
        
        dn = DatasetNumber(62**3-1,None,'unregistered')
        self.assertEquals('d0000ZZZ', str(dn))

        dn = DatasetNumber(62**3-1,None,'self')
        self.assertEquals('d000000ZZZ', str(dn))
        
        tn = TableNumber(dn, 1)
        print str(tn)


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

        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-segment', 
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-segment-0.0.1', 
                          part_name.vname)

        part_name = part_name.clone()
        
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-segment', 
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-segment-0.0.1', 
                          part_name.vname)        

        # Name Query

        name_query = NameQuery(source='source.com', dataset='dataset', vname='foobar', type=NameQuery.NONE)

        with self.assertRaises(NotImplementedError):
            name_query.path

        d = name_query.dict

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        name_query = name_query.clone()

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        name_query_2 = name_query.with_none()
        
        self.assertEquals(None,name_query_2.dict['type'])

        # With a semantic version spec
        
        name = Name(source='source.com', dataset = 'dataset', version='0.0.1')
        self.assertEquals('source.com-dataset-orig-0.0.1',name.vname)  
        
        name.version_major = 2
        name.version_build = ('foobar',)

        self.assertEquals('source.com-dataset-orig-2.0.1+foobar',name.vname)  
        
        name = Name(source='source.com', dataset='dataset',variation='variation', version='>=0.0.1')

        self.assertEquals('source.com-dataset-variation->=0.0.1',name.vname)   

        name = Name(source='source.com', dataset='dataset',variation='variation', version='0.0.1')

        self.assertEquals('source.com/dataset-variation-0.0.1',name.path)   

        self.assertEquals('source.com/dataset-variation',name.source_path) 

        self.assertEquals('source.com/dataset-variation-0.0.1.db',name.cache_key) 

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment='segment',
                                  **name.dict
                                  )


        self.assertEquals('source.com/dataset-variation-0.0.1/table/time-space/grain-segment',part_name.path)   
        self.assertEquals('source.com/dataset-variation/table/time-space/grain-segment',part_name.source_path)  
         
        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  format='db',
                                  **name.dict
                                  )


        self.assertEquals('source.com-dataset-variation-table-time-space',part_name.name)
        self.assertEquals('source.com-dataset-variation-table-time-space-0.0.1',part_name.vname)
        self.assertEquals('source.com/dataset-variation-0.0.1/table/time-space',part_name.path)   
        self.assertEquals('source.com/dataset-variation/table/time-space',part_name.source_path)   

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  format='format',
                                  **name.dict
                                  )
   
        self.assertEquals('source.com/dataset-variation-0.0.1/time-space',part_name.path) 
        self.assertEquals('source.com/dataset-variation/time-space',part_name.source_path) 


        pname = PartialPartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  format='format'
                                  )

        part_name = pname.promote(name)
        
        self.assertEquals('source.com-dataset-variation-table-time-space-format-0.0.1',part_name.vname) 

        
        

    def test_identity(self):

        name = Name(source='source.com', dataset='foobar',  version='0.0.1')
        dn = DatasetNumber(10000, 1, assignment_class='registered')
        
        ident = Identity(name, dn)

        self.assertEquals('d002Bi',ident.id_)   
        self.assertEquals('d002Bi001',ident.vid)   
        self.assertEquals('source.com-foobar-orig',str(ident.name))   
        self.assertEquals('source.com-foobar-orig-0.0.1',ident.vname)   
        self.assertEquals('source.com-foobar-orig-0.0.1~d002Bi001',ident.fqname)   
        self.assertEquals('source.com/foobar-orig-0.0.1',ident.path) 
        self.assertEquals('source.com/foobar-orig',ident.source_path) 
        self.assertEquals('source.com/foobar-orig-0.0.1.db',ident.cache_key)

        self.assertEquals('source.com-foobar-orig-0.0.1', ident.name.dict['vname'])
        
        self.assertEquals(set(['id','vid','revision','name', 'vname', 'creator',
                               'variation', 'dataset', 'source', 'version']), 
                          set(ident.dict.keys()))
        
        part_name = PartitionName(time = 'time',
                                  space='space',
                                  format='format',
                                  **name.dict
                                  )
        
        pn = PartitionNumber(dn, 500)
        
        ident = Identity(part_name, pn)
        
        self.assertEquals(set(['id','vid','revision', 'creator',
                               'name', 'vname', 'space', 'format', 
                               'variation', 'dataset', 'source', 
                               'version', 'time']), 
                          set(ident.dict.keys()))
        
        self.assertEquals('p002Bi084',ident.id_)   
        self.assertEquals('p002Bi084001',ident.vid)   
        self.assertEquals('source.com-foobar-orig-time-space-format',str(ident.name))   
        self.assertEquals('source.com-foobar-orig-time-space-format-0.0.1',ident.vname)   
        self.assertEquals('source.com-foobar-orig-time-space-format-0.0.1~p002Bi084001',ident.fqname)   
        self.assertEquals('source.com/foobar-orig-0.0.1/time-space',ident.path) 
        self.assertEquals('source.com/foobar-orig/time-space',ident.source_path) 
        self.assertEquals('source.com/foobar-orig-0.0.1/time-space.db',ident.cache_key)
        
        # Updating partition names that were partially specified
        
        pnq = PartitionNameQuery(time = 'time',
                          space='space',
                          format='format'
                          )
        #import pprint
        #pprint.pprint(pnq.dict)
        
        
    def test_partial_name(self):
        pass
        
    def test_resolve(self):
        from  testbundle.bundle import Bundle
        from sqlalchemy.exc import IntegrityError
        
        bundle = Bundle()  
        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()
        
        bp = bundle.partitions
 
        with bundle.session:
            bp._new_orm_partition(PartialPartitionName(time = 't1', space='s1'))
            bp._new_orm_partition(PartialPartitionName(time = 't1', space='s2'))
            bp._new_orm_partition(PartialPartitionName(time = 't1', space=None))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space='s1'))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space='s2'))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space=None))
     
            
        with self.assertRaises(IntegrityError):
            with bundle.session: 
                bp._new_orm_partition(PartialPartitionName(time = 't1', space='s1'))    
            
        pnq = PartitionNameQuery(time=NameQuery.ANY, space='s1')
            
        names = [p.vname
                 for p in bp._find_orm(pnq).all()]


        self.assertEqual(set([u'source-dataset-subset-variation-t2-s1-0.0.1', 
                              u'source-dataset-subset-variation-t1-s1-0.0.1']),
                         set(names))
        
        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(space=NameQuery.ANY)).all()]

        self.assertEqual(6,len(names))

        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.ANY)).all()]

        self.assertEqual(set(['source-dataset-subset-variation-t1-s2-0.0.1', 
                              'source-dataset-subset-variation-t1-0.0.1', 
                              'source-dataset-subset-variation-t1-s1-0.0.1']),
                         set(names))
        

        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.NONE)).all()]

        self.assertEqual(set(['source-dataset-subset-variation-t1-0.0.1']),
                         set(names))

        # Start over, use a higher level function to create the partitions
        
        bundle = Bundle()  
        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()
        bp = bundle.partitions

        bp._new_partition(PartialPartitionName(time = 't1', space='s1'))
        self.assertEquals(1, len(bp.all))
        
        bp._new_partition(PartialPartitionName(time = 't1', space='s2'))
        self.assertEquals(2, len(bp.all))
        
        bp._new_partition(PartialPartitionName(time = 't1', space=None))
        bp._new_partition(PartialPartitionName(time = 't2', space='s1'))
        bp._new_partition(PartialPartitionName(time = 't2', space='s2'))
        bp._new_partition(PartialPartitionName(time = 't2', space=None))
        self.assertEquals(6, len(bp.all))
        
        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.ANY)).all()]

        self.assertEqual(set(['source-dataset-subset-variation-t1-s2-0.0.1', 
                              'source-dataset-subset-variation-t1-0.0.1', 
                              'source-dataset-subset-variation-t1-s1-0.0.1']),
                         set(names))
       
       
        # Start over, use a higher level function to create the partitions
        
        bundle = Bundle()  
        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()
        bp = bundle.partitions
     
        p = bp.new_db_partition(time = 't1', space='s1')
        self.assertEquals('source-dataset-subset-variation-t1-s1-0.0.1~p1DxuZ001001', p.identity.fqname)
        
        p = bp.find_or_new(time = 't1', space='s2')
        self.assertEquals('source-dataset-subset-variation-t1-s2-0.0.1~p1DxuZ002001', p.identity.fqname)

        # Duplicate
        p = bp.find_or_new(time = 't1', space='s2')
        self.assertEquals('source-dataset-subset-variation-t1-s2-0.0.1~p1DxuZ002001', p.identity.fqname)
        
        p = bp.find_or_new_hdf(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-hdf-0.0.1~p1DxuZ003001', p.identity.fqname)
        
        p = bp.find_or_new_csv(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-csv-0.0.1~p1DxuZ004001', p.identity.fqname)
        
        p = bp.find_or_new_geo(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-geo-0.0.1~p1DxuZ005001', p.identity.fqname)
        
 
        # Ok! Build!
 
        bundle = Bundle()  
        bundle.exit_on_fatal = False
        
        bundle.clean()
        bundle.pre_prepare()
        bundle.prepare()
        bundle.post_prepare()
        bundle.pre_build()
        bundle.build_db_inserter_codes()
        bundle.post_build()
                
        self.assertEquals('d1DxuZ001',bundle.identity.vid) 
        self.assertEquals('source-dataset-subset-variation',bundle.identity.sname) 
        self.assertEquals('source-dataset-subset-variation-0.0.1',bundle.identity.vname) 
        self.assertEquals('source-dataset-subset-variation-0.0.1~d1DxuZ001',bundle.identity.fqname)

    def test_number_service(self):
        
        ## For this test, setup these access keys in the
        ## Redis Server:
        ##
        ## redis-cli set assignment_class:test-ac-authoritative authoritative
        ## redis-cli set assignment_class:test-ac-registered registered
        
        from databundles.identity import NumberServer
        from databundles.run import  get_runconfig
        rc = get_runconfig()
    
        ng = rc.group('numbers')

        ns = NumberServer(host=ng['host'], port=ng['port'], key='test-ac-registered')

        n = ns.next()
        self.assertEqual(6,len(str(n)))

        # Next request is authoritative, so no need to sleep here.

        ns = NumberServer(host=ng['host'], port=ng['port'], key='test-ac-authoritative')

        n = ns.next()
        self.assertEqual(4,len(str(n)))

        ns.sleep() # Avoid being rate limited

        ns = NumberServer(host=ng['host'], port=ng['port'])
        n = ns.next()
        self.assertEqual(8,len(str(n)))

        # Try it with the key assigned in the configuration.

        ns = NumberServer(**get_runconfig().group('numbers'))
        print ns.next()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()