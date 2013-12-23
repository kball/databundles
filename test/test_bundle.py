'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.identity import * #@UnusedWildImport
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir


        
    def save_bundle(self):
        pass
        
    def restore_bundle(self):
        pass 
      
    def test_objectnumber(self):
          
        values = ['a17PY5','c17PY50a','d17PY50a0a','b17PY500a']
        
        for v in values:
            x = ObjectNumber.parse(v)   
            self.assertEquals(v, str(x))
        
        dn = DatasetNumber()
        
        base = str(dn)[1:]
      
        tn = TableNumber(dn, 10)
        self.assertEquals('c'+base+'0a',str(tn))
        
        cn = ColumnNumber(tn, 20)
        self.assertEquals('d'+base+'0a00k',str(cn))
        
        pn = PartitionNumber(dn, 30)
        self.assertEquals('b'+base+'00u',str(pn))
        
        return True
      
        self.assertEquals('a1',str(ObjectNumber(1)))
        self.assertEquals('b101',str(ObjectNumber(1,1)))
        self.assertEquals('c10101',str(ObjectNumber(1,1,1)))

        with self.assertRaises(ValueError):
            self.assertEquals('aFooBar',str(ObjectNumber('FooBar')))
      
        
        self.assertEquals('aFooBar',str(ObjectNumber('aFooBar')))
        self.assertEquals('aFooBar',str(ObjectNumber(ObjectNumber('aFooBar'))))
 
        on = ObjectNumber('aFooBar')

        self.assertEquals('bFooBar00',str(ObjectNumber(on,0)))
        self.assertEquals('cFooBar0000',str(ObjectNumber(on,0,0)))
        self.assertEquals('bFooBarZZ',str(ObjectNumber(on,3843)))
        self.assertEquals('cFooBarZZZZ',str(ObjectNumber(on,3843,3843)))
        
        with self.assertRaises(ValueError):
            on = ObjectNumber(on,3844)
            print str(on)
     
        with self.assertRaises(ValueError):
            on = ObjectNumber(on,3844,3844)
            print str(on)
     
        o = ObjectNumber('aFooBar')
        self.assertIsNone(o.table);
        self.assertIsNone(o.column);
        
        o = ObjectNumber('bFooBar03')
        self.assertEquals(3,o.table);
        self.assertIsNone(o.column);
        
        o = ObjectNumber('cFooBar0302')
        self.assertEquals(3,o.table);
        self.assertEquals(2,o.column);
        
        o = ObjectNumber('cFooBar0302',20)
        o.type = ObjectNumber.TYPE.TABLE
        self.assertEquals(20,o.table);
        self.assertEquals('bFooBar0k',str(o))
        
      
    def test_identity(self):
        from databundles.identity import new_identity
        self.assertEqual('source', self.bundle.identity.source)
        self.assertEqual('dataset', self.bundle.identity.dataset)
        self.assertEqual('subset', self.bundle.identity.subset)
        self.assertEqual('variation', self.bundle.identity.variation)
        self.assertEqual('creator', self.bundle.identity.creator)
        self.assertEqual(1, int(self.bundle.identity.revision))
        self.assertEqual('source-dataset-subset-variation-ca0d', 
                         self.bundle.identity.name)


        pid = self.bundle.identity
        d = pid.to_dict()
        
        self.assertEquals('source-dataset-subset-variation-ca0d', str(new_identity(pid.to_dict())))
        
        d['id'] = 'foobar'
        self.assertRaises(ValueError, new_identity, (d))
        
        del d['id']
        
        self.assertEquals('source-dataset-subset-variation-ca0d',str(new_identity(d)))
        self.assertEquals('source-dataset-subset-variation-ca0d.grain',str(new_identity( {'grain':'grain'}, bundle=self.bundle)))
        
        d['grain'] = 'grain'
        self.assertEquals('source-dataset-subset-variation-ca0d.grain',str( new_identity(d)))
        
   

    def test_db_bundle(self):
        
        from databundles.bundle import BuildBundle, DbBundle
        
        b = BuildBundle(self.bundle_dir)
        b.clean()
        
        self.assertTrue(b.identity.id_ is not None)
        self.assertEquals('source-dataset-subset-variation-ca0d', b.identity.name)
        self.assertEquals('source-dataset-subset-variation-ca0d-r1', b.identity.vname)
        
        b.database.create()
        
        db_path =  b.database.path
        
        dbb = DbBundle(db_path)
        
        self.assertEqual("source-dataset-subset-variation-ca0d", dbb.identity.name)
        self.assertEqual("source-dataset-subset-variation-ca0d-r1", dbb.identity.vname)
        self.assertEqual("source-dataset-subset-variation-ca0d", dbb.config.identity.name)

    def test_paths(self):
        ''' Test that abuild bundle and a db bundle both produce the same paths. '''
        
        from databundles.bundle import BuildBundle, DbBundle
        
        b = self.bundle
        db  = DbBundle(b.database.path)

        self.assertEqual(b.path, db.path)
        self.assertTrue(os.path.exists(b.path))
        
        self.assertEqual( b.database.path, db.database.path)
        self.assertTrue(os.path.exists(b.database.path))

        self.assertEqual( b.identity.path, db.identity.path)

        for p in zip(b.partitions, db.partitions):
            self.assertEqual(p[0].path, p[1].path)
            self.assertTrue(p[0].path)
     
    def test_schema_direct(self):
        '''Test adding tables directly to the schema'''
        
        # If we don't explicitly set the id_, it will change for every run. 
        self.bundle.config.identity.id_ = 'aTest'

        self.bundle.schema.clean()

        with self.bundle.session:
            s = self.bundle.schema
            s.add_table('table 1', altname='alt name a')
            s.add_table('table 2', altname='alt name b')
            
            self.assertRaises(Exception,  s.add_table, ('table 1', ))
          
            
            t = s.add_table('table 3', altname='alt name')
        
            s.add_column(t,'col 1',altname='altname1')
            s.add_column(t,'col 2',altname='altname2')
            s.add_column(t,'col 3',altname='altname3')

        
        #print self.bundle.schema.as_csv()
        
        self.assertIn('c1DxuZ01', [t.id_ for t in self.bundle.schema.tables])
        self.assertIn('c1DxuZ02', [t.id_ for t in self.bundle.schema.tables])
        self.assertNotIn('cTest03', [t.id_ for t in self.bundle.schema.tables])
    
        t = self.bundle.schema.table('table_3')
    
        self.assertIn('d1DxuZ03001', [c.id_ for c in t.columns])
        self.assertIn('d1DxuZ03002', [c.id_ for c in t.columns])
        self.assertIn('d1DxuZ03003', [c.id_ for c in t.columns])
        
        # Try with a nested session, b/c we need to test it somewhere ... 
        with self.bundle.session:
            
            with self.bundle.session:
                
                t = s.add_table('table 4', altname='alt name')
            
                s.add_column(t,'col 1',altname='altname1')
                s.add_column(t,'col 2',altname='altname2')
                s.add_column(t,'col 3',altname='altname3')

        
    def x_test_generate_schema(self):
        '''Uses the generateSchema method in the bundle'''
        from databundles.orm import  Column
        
        with self.bundle.session:
            s = self.bundle.schema
            s.clean()
            
            t1 = s.add_table('table1')
                    
            s.add_column(t1,name='col1', datatype=Column.DATATYPE_REAL )
            s.add_column(t1,name='col2', datatype=Column.DATATYPE_INTEGER )
            s.add_column(t1,name='col3', datatype=Column.DATATYPE_TEXT )  
            
            t2 = s.add_table('table2')   
            s.add_column(t2,name='col1' )
            s.add_column(t2,name='col2' )
            s.add_column(t2,name='col3' )   
    
            t3 = s.add_table('table3') 
            s.add_column(t3,name='col1', datatype=Column.DATATYPE_REAL )
            s.add_column(t3,name='col2', datatype=Column.DATATYPE_INTEGER )
            s.add_column(t3,name='col3', datatype=Column.DATATYPE_TEXT )   

     
    def test_names(self):
        print 'Testing Names'

     
    def test_column_processor(self):
        from databundles.orm import  Column
        from databundles.transform import BasicTransform, CensusTransform
        
        
        
        self.bundle.schema.clean()
        
        with self.bundle.session:
            s = self.bundle.schema  

            t = s.add_table('table3') 
            s.add_column(t,name='col1', datatype=Column.DATATYPE_INTEGER, default=-1, illegal_value = '999' )
            s.add_column(t,name='col2', datatype=Column.DATATYPE_TEXT )   
            s.add_column(t,name='col3', datatype=Column.DATATYPE_REAL )

        
            c1 = t.column('col1')
    
            
            self.assertEquals(1, BasicTransform(c1)({'col1': ' 1 '}))
            
            with self.assertRaises(ValueError):
                print "PROCESSOR '{}'".format(CensusTransform(c1)({'col1': ' B '}))
            
            self.assertEquals(1, CensusTransform(c1)({'col1': ' 1 '}))
            self.assertEquals(-1, CensusTransform(c1)({'col1': ' 999 ' }))
            self.assertEquals(-3, CensusTransform(c1)({'col1': ' # '}))
            self.assertEquals(-2, CensusTransform(c1)({'col1': ' ! '}))
       
       
    def test_validator(self):
       
        #
        # Validators
        #
        
        
        tests =[
                ( 'tone',True, (None,'VALUE',0,0) ),
                ( 'tone',True, (None,'VALUE',-1,0) ),
                ( 'tone',False, (None,'DEFAULT',0,0) ),
                ( 'tone',False, (None,'DEFAULT',-1,0) ),
                
                ( 'ttwo',True, (None,'DEFAULT',0,0) ),
                ( 'ttwo',True, (None,'DEFAULT',0,3.14) ),
                ( 'ttwo',False, (None,'DEFAULT',-1,0) ),
                
                ( 'tthree',True, (None,'DEFAULT',0,0) ),
                ( 'tthree',True, (None,'DEFAULT',0,3.14) ),

                ( 'all',True, (None,'text1','text2',1,2,3,3.14)),
                ( 'all',False, (None,'text1','text2',-1,-1,3,3.14)),
                ( 'all',False, (None,'text1','text2',-1,2,3,3.14)),
                ( 'all',False, (None,'text1','text2',1,-1,3,3.14)),
              ]
     
        for i, test in enumerate(tests): 
            table_name, truth, row = test
            table =  self.bundle.schema.table(table_name);
            vd = table._get_validator()
            
            if truth:
                self.assertTrue(vd(row), "Test {} not 'true' for table '{}': {}".format(i+1,table_name,row))
                
            else:
                self.assertFalse(vd(row), "Test {} not 'false' for table '{}': {}".format(i+1,table_name,row))

        # Testing the "OR" join of multiple columns. 

        tests =[
                ( 'tone',True, (None,'VALUE',0,0) ), #1
                ( 'tone',True, (None,'VALUE',-1,0) ),
                ( 'tone',False, (None,'DEFAULT',0,0) ),
                ( 'tone',False, (None,'DEFAULT',-1,0) ),
                
                ( 'ttwo',True, (None,'DEFAULT',0,0) ), #5
                ( 'ttwo',True, (None,'DEFAULT',0,3.14) ),
                ( 'ttwo',False, (None,'DEFAULT',-1,0) ),
                
                ( 'tthree',True, (None,'DEFAULT',0,0) ), #8
                ( 'tthree',True, (None,'DEFAULT',0,3.14) ),
                
                ( 'all',True, (None,'text1','text2',1,2,3,3.14)), #10
                ( 'all',False, (None,'text1','text2',-1,-1,3,3.14)), #11
                ( 'all',True, (None,'text1','text2',-1,2,3,3.14)), #12
                ( 'all',True, (None,'text1','text2',1,-1,3,3.14)), #13
              ]
     
        for i, test in enumerate(tests): 
            table_name, truth, row = test
            table =  self.bundle.schema.table(table_name);
            vd =table._get_validator(and_join=False)
            if truth:
                self.assertTrue(vd(row), "Test {} not 'true' for table '{}': {}".format(i+1, table_name,row))
            else:
                self.assertFalse(vd(row), "Test {} not 'false' for table '{}': {}".format(i+1, table_name,row))

        
        # Test the hash functions. This test depends on the d_test values in geoschema.csv
        tests =[
        ( 'tone','A|1|', (None,'A',1,2) ), 
        ( 'ttwo','1|2|', (None,'B',1,2) ), 
        ( 'tthree','C|2|', (None,'C',1,2) )]
        
        import hashlib
        
        for i, test in enumerate(tests): 
            table_name, hashed_str, row = test
            table =  self.bundle.schema.table(table_name);
           
            m = hashlib.md5()
            m.update(hashed_str)
            
            self.assertEquals(int(m.hexdigest()[:14], 16), table.row_hash(row))
        
    def test_partition(self):
        
        from  databundles.partition import  PartitionIdentity

        ## TODO THis does does not test the 'table' parameter of the ParitionId
          
        pid1 = PartitionIdentity(self.bundle.identity, time=10, space=10)
        pid2 = PartitionIdentity(self.bundle.identity, time=20, space=20)
        pid3 = PartitionIdentity(self.bundle.identity, space=30,)
        
        
        self.bundle.partitions.new_db_partition(pid1, data={'pid':'pid1'})
        self.bundle.partitions.new_db_partition(pid2, data={'pid':'pid2'})
        self.bundle.partitions.new_db_partition(pid3, data={'pid':'pid3'})
        self.bundle.partitions.new_db_partition(pid1, data={'pid':'pid1'})
        self.bundle.partitions.new_db_partition(pid2, data={'pid':'pid21'})
        self.bundle.partitions.new_db_partition(pid3, data={'pid':'pid31'})

        
        # 4 partitions from the build ( defined in meta/geoschema.csv),
        # three we just created. 


        self.assertEqual(28, len(self.bundle.partitions.all))
        
        p = self.bundle.partitions.new_db_partition(pid1)
        p.database.create() # Find will go to the library if the database doesn't exist. 
        self.assertEquals('pid1',p.data['pid'] )
      
        p = self.bundle.partitions.new_db_partition(pid2) 
        p.database.create()  
        self.assertEquals('pid2',p.data['pid'] ) 

        p = self.bundle.partitions.new_db_partition(pid3)
        p.database.create()   
        self.assertEquals('pid3',p.data['pid'] ) 

        p = self.bundle.partitions.find(pid1)   
        self.assertEquals('pid1',p.data['pid'] )
      
        p = self.bundle.partitions.find(pid2)   
        self.assertEquals('pid2',p.data['pid'] ) 

        p = self.bundle.partitions.find(pid3)   
        self.assertEquals('pid3',p.data['pid'] ) 
         
        with self.bundle.session:
            p = self.bundle.partitions._find_orm(pid3).first() 
            s = self.bundle.database.session
            p.data['foo'] = 'bar'
    
            
        p = self.bundle.partitions.find(pid3)  
        print p.data 
        self.assertEquals('bar',p.data['foo'] ) 
       
        s.commit()
        p.database.create()
        
        p = self.bundle.partitions.find('source-dataset-subset-variation-ca0d.30')
        self.assertTrue(p is not None)
        self.assertEquals(pid3.name, p.identity.name)
 
        #
        # Create all possible combinations of partition names
        # 
        s = set()
        table = self.bundle.schema.tables[0]
        
        p = (('time','time1'),('space','space2'),('table',table.name),('grain','grain4'))
        p += p
        pids = []
        for i in range(4):
            for j in range(4):
                s.add(p[i:i+j+1])
            
        for v in s:
            pid = PartitionIdentity(self.bundle.identity,**dict(v))
            pids.append(pid)
        
        with self.bundle.session as s:
        
            # These two deletely bits clear out all of the old
            # partitions, to avoid a conflict with the next section. We also have
            # to delete the files, since create() adds a partition record to the database, 
            # and if one arealy exists, it will throw and Integrity Error. 
            for p in self.bundle.partitions:
                if os.path.exists(p.database.path):
                    os.remove(p.database.path)
            
            for p in self.bundle.dataset.partitions:
                s.delete(p)

        for pid in pids:

            part = self.bundle.partitions.new_db_partition(pid)
            part.create()
            
            parts = self.bundle.partitions._find_orm(pid).all()
            self.assertIn(pid.name, [p.name for p in parts])

        
    def test_runconfig(self):
        '''Check the the RunConfig expands  the library configuration'''
        from databundles.run import  get_runconfig, RunConfig
        
        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))

        l = rc.library('library1')
         
        self.assertEquals('database1', l['database']['_name'])
        self.assertEquals('filesystem1', l['filesystem']['_name'])
        self.assertEquals('filesystem2', l['filesystem']['upstream']['_name'])
        self.assertEquals('filesystem3', l['filesystem']['upstream']['upstream']['_name'])
        self.assertEquals('devtest.sandiegodata.org', l['filesystem']['upstream']['upstream']['account']['_name'])
        
    def x_test_tempfile(self):
  
        self.test_generate_schema()
  
        table = self.bundle.schema.tables[0]
        print "TABLE", table.name
        tf = self.bundle.database.tempfile(table)
        
        print "PATH",tf.path
        w = tf.writer
        
        for i in range(10):
            w.writerow([i,i,i])

      
    def test_make_bundles(self):        
        bundle = Bundle()
        bundle.clean()
        bundle = Bundle()   
        bundle.exit_on_fatal = False
        bundle.prepare()
        bundle.build()



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())