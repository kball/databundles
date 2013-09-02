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
     
        for table in self.schema.tables:   
            pid = PartitionIdentity(self.identity, table=table.name)
            if table.data.get('make_partition', False):
                partition = self.partitions.new_db_partition(pid)  
                partition.create_with_tables(table.name)
  
        self.schema.create_tables()
  
        return True
  
    def build(self):
        import random
        from functools import partial
        import numpy as np
        
        sink = self.database.path
     
        #self.log("Writing random data to: "+ self.database.path)
        fields = [
                  ('tone_id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random)
                  ]
        f = petl.dummytable(10000,fields) #@UndefinedVariable
        f.tosqlite3(sink, 'tone', create=False) 
        f.tosqlite3(sink, 'ttwo', create=False) 
        f.tosqlite3(sink, 'tthree', create=False)
      

        # CSV
        self.build_csv()
        
        return True
        
        self.build_db()
        
        self.build_geo()
        
        self.build_hdf()
        

        return True


    def build_db(self):
   
        # Now write random data to each of the pable partitions. 
        
        for partition in self.partitions.all:
            if partition.table.name in ('tone','ttwo','tthree'):  
                #self.log("Loading "+partition.name)
                db = partition.database.path
                table_name = partition.table.name
                petl.dummytable(30000,fields).tosqlite3(db, table_name, create=False) #@UndefinedVariable

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
                    ins.insert({'name': str(lon)+';'+str(lat), 'wkt':"POINT({} {})".format(lon,lat)})
        
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



        