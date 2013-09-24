'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import BuildBundle
import petl.fluent as petl

class Bundle(BuildBundle):
    

    def prepare(self):
        from databundles.partition import PartitionIdentity 
        
        super(self.__class__, self).prepare()
    
        return True
  
    @property
    def fields(self):
        from functools import partial
        import random
        return   [
                  ('tone_id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random),
                  ]
  
    @property
    def fields2(self):
        from functools import partial
        import random
        return   [
                  ('tone_id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random),
                  ('extra', lambda: None),
                  ('extra2', lambda: None),
                  ]
  
    def build(self):

        with self.session:
            
            self.log("Build geo")
            self.build_geo()
            
            self.log("Build missing")
            self.build_with_missing()
            

            self.log("Build csvsegments")
            self.build_csvsegments()

            self.log("Build csv")
            self.build_csv()

            self.log("Build db")
            self.build_db()


            self.log("Build hdf")
            self.build_hdf()

        return True


    def build_csvsegments(self):
        
        p = self.partitions.find_or_new_db(table="tone")
        
        with p.database.csvinserter(segment_rows=100) as ins:
            for i in range(1000):
                ins.insert((None,"str"+str(i),i,i))
        
    def build_with_missing(self):
        
        p = self.partitions.find_or_new_db(table="tone", grain='missing')
        
        with p.database.inserter('tone') as ins:
            for i in range(1000):
                ins.insert({ 'tone_id':None,
                             'text':"str"+str(i),
                             'integer':i,
                             'float':i})
        

    def build_db(self):

        # Now write random data to each of the pable partitions. 
        
        for table_name in  ('tone',):
            p = self.partitions.find_or_new_db(table=table_name)
            petl.dummytable(30000,self.fields2).tosqlite3(p.database.path, table_name, create=False) #@UndefinedVariable

        
        for table_name in  ('ttwo',):
            p = self.partitions.find_or_new_db(table=table_name)
            petl.dummytable(30000,self.fields).tosqlite3(p.database.path, table_name, create=False) #@UndefinedVariable

        for seg in range(1,5):
            p = self.partitions.find_or_new_db(table='tthree', segment=seg)
            petl.dummytable(30000,self.fields).tosqlite3(p.database.path, 'tthree', create=False) #@UndefinedVariable
            p.write_stats()

    def build_geo(self):
   
        # Create other types of partitions. 
        geot1 = self.partitions.find_or_new_geo(table='geot1')
        

        with geot1.database.inserter() as ins:
            for lat in range(10):
                for lon in range(10):
                    ins.insert({'name': str(lon)+';'+str(lat), 'lon':lon, 'lat':lat})
        
        # Create other types of partitions. 
        geot2 = self.partitions.find_or_new_geo(table='geot2')
     
        
        with geot2.database.inserter() as ins:
            for lat in range(10):
                for lon in range(10):
                    ins.insert({'name': "Centroid(POINT({} {}))".format(lon,lat),
                                'wkt':"POINT({} {})".format(lon,lat)})
        
    def build_hdf(self):
        import numpy as np
        hdf = self.partitions.find_or_new_hdf(table='hdf5')

        a = np.zeros((10,10))
        for y in range(10):
            for x in range(10):
                a[x,y] = x*y
 
        ds = hdf.database.create_dataset('hdf', data=a, compression=9)
        hdf.database.close()       

    def build_csv(self):
        from databundles.identity import Identity
        
        for j in range(1,5):
            csvt = self.partitions.find_or_new_csv(table='csv', segment=j)
            lr = self.init_log_rate(2500, "Segment "+str(j))
            with csvt.database.inserter(skip_header=True) as ins:
                for i in range(5000):
                    r = [i,'foo',i, float(i)* 37.452]
                    ins.insert(r)
                    lr()

    def fetch(self):
        pass



        