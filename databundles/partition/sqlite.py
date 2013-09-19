"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import PartitionBase, PartitionIdentity
from ..database.partition import  PartitionDb
   
class SqlitePartitionIdentity(PartitionIdentity):
    PATH_EXTENSION = '.db'
    pass
  
 
class SqlitePartition(PartitionBase):
    '''Represents a bundle partition, part of the bundle data broken out in 
    time, space, or by table. '''
    
    FORMAT = 'db'
    
    def __init__(self, bundle, record, memory=False, **kwargs):
        
        super(SqlitePartition, self).__init__(bundle, record)
        self.memory  = memory
        self.format = self.FORMAT


    @property
    def database(self):
        if self._database is None:
            self._database = PartitionDb(self.bundle, self, base_path=self.path, memory=self.memory)          
        return self._database



    def query(self,*args, **kwargs):
        """Convience function for self.database.query()"""
     
        return self.database.query(*args, **kwargs)
    
    def create_with_tables(self, tables=None, clean=False):
        '''Create, or re-create,  the partition, possibly copying tables
        from the main bundle
        
        Args:
            tables. String or Array of Strings. Specifies the names of tables to 
            copy from the main bundle. 
            
            clean. If True, delete the database first. Defaults to true. 
        
        '''

        if not tables: 
            raise ValueError("'tables' cannot be empty")

        if not isinstance(tables, (list, tuple)):
            tables = [tables]

        if clean:
            self.database.delete()

        self.database.create()

        self.add_tables(tables)
        
        
    def add_tables(self,tables):

        for t in tables:
            if not t in self.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t) #@UnusedVariable
                table.create(bind=self.database.engine)       

    def create(self):

        tables = self.data.get('tables',[])

        if tables:
            self.create_with_tables(tables=tables)
        else:
            self.database.create()


    def csvize(self, logger=None, store_library=False, write_header=False):
        '''Convert this partition to CSV files that are linked to the partition'''
        
        if not self.record.count:
            raise Exception("Must run stats before cvsize")
        
        BYTES_PER_CELL = 3.8 # Bytes per num_row * num_col, experimental
        
        # Shoot for about 250M uncompressed, which should compress to about 25M

        rows_per_seg = (250*1024*1024 / (len(self.table.columns) * BYTES_PER_CELL) ) 
        
        # Round up to nearest 100K
        
        rows_per_seg = round(rows_per_seg/100000+1) * 100000
        
        if logger:
            logger.always("Csvize: {} rows per segment".format(rows_per_seg))
        
        ins  =  None
        p = None
        seg = 0
        ident = None
        count = 0
        min_key = max_key = None

        pk = self.table.primary_key.name

        def store_library(p):
            if store_library:
                if logger:
                    logger.always("Storing {} to Library".format(p.identity.name), now=True)
                    
                dst, _,_ = self.bundle.library.put(p)
                p.database.delete()
                
                if logger:
                    logger.always("Stored at {}".format(dst), now=True)            

        for i,row in enumerate(self.rows):

            if not min_key:
                min_key = row[pk]

            if i % rows_per_seg == 0:
                      
                if p: # Don't do it on the first record. 
                    p.write_stats(min_key, max_key, count)
                    count = 0
                    min_key = row[pk]
                    ins.close()

                    store_library(p)

                seg += 1
                ident = self.identity
                ident.segment = seg

                p = self.bundle.partitions.find_or_new_csv(ident)
                ins = p.inserter( write_header=write_header)
                logger.always("New CSV Segment: {}".format(p.identity.name), now=True)
                
            count += 1
            ins.insert(dict(row))
            max_key = row[pk]
       
            if logger:
                logger("CSVing for {}".format(ident.name))

        p.write_stats(min_key, max_key, count)
        ins.close()
        store_library(p)

    @property
    def rows(self):
        
        pk = self.table.primary_key.name
        return self.database.query("SELECT * FROM {} ORDER BY {} ".format(self.table.name,pk))
        

    def write_stats(self):
        
        t = self.table
        
        if not t:
            return
        
        s = self.database.session
        self.record.count = s.execute("SELECT COUNT(*) FROM {}".format(t.name)).scalar()
     
        self.record.min_key = s.execute("SELECT MIN({}) FROM {}".format(t.primary_key.name,t.name)).scalar()
        self.record.max_key = s.execute("SELECT MAX({}) FROM {}".format(t.primary_key.name,t.name)).scalar()
     
        bs = self.bundle.database.session
        bs.merge(self.record)
        bs.commit()
        


    def __repr__(self):
        return "<db partition: {}>".format(self.identity.vname)
