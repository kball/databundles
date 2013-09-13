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
    
    def __init__(self, bundle, record):
        
        super(SqlitePartition, self).__init__(bundle, record)
        self.format = self.FORMAT


    @property
    def database(self):
        if self._database is None:
            self._database = PartitionDb(self.bundle, self, base_path=self.path)          
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


    def csvize(self, logger=None, store_library=False):
        '''Convert this partition to CSV files that are linked to the partition'''
        
        if not self.record.count:
            raise Exception("Must run stats before cvsize")
        
        BYTES_PER_CELL = 3.8 # Bytes per num_row * num_col, experimental
        
        # Shoot for about 500M uncompressed, which should compress to about 100M

        rows_per_seg = (512*1024*1024 / (len(self.table.columns) * BYTES_PER_CELL) ) 
        
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

        for i,row in enumerate(self.rows):

            if not min_key:
                min_key = row[pk]

            if i % rows_per_seg == 0:
                print "Next Segment"
                
                if p: # Don't do it on the first record. 
                    p.write_stats(min_key, max_key, count)
                    count = 0
                    min_key = row[pk]
                    ins.close()

                    if store_library:
                        if logger:
                            logger.always("Storing {} to Library".format(p.vname), now=True)
                            
                        dst, cache_key, url = self.library.put(p)
                        p.database.delete()
                        
                        if logger:
                            logger.always("Stored at {}".format(dst), now=True)

                seg += 1
                ident = self.identity
                ident.segment = seg

                p = self.bundle.partitions.find_or_new_csv(ident)
                ins = p.inserter()
                
            count += 1
            ins.insert(dict(row))
            max_key = row[pk]
       
            if logger:
                logger("CSVing for {}".format(ident.name))

        p.write_stats(min_key, max_key, count)
        ins.close()

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
        return "<db partition: {}>".format(self.vname)
