"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlite import Database

class PartitionDb(Database):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, bundle, partition, base_path, **kwargs):
        '''''' 
        
        super(PartitionDb, self).__init__(bundle, base_path, **kwargs)  
        self.partition = partition
    
    @property
    def name(self):
        return self.partition.name

    @property 
    def path(self):
        return self.base_path + self.EXTENSION
    
    def sub_dir(self, *args):
        return  self.bundle.sub_dir(*args)
    

    def inserter(self, table_or_name=None,**kwargs):
        
        if table_or_name is None and self.table:
            table_or_name = self.partition.identity.table

        return super(PartitionDb, self).inserter(table_or_name, **kwargs)
        
    
    def create(self, copy_tables = True):
        from databundles.orm import Dataset
        from databundles.orm import Table
        
        '''Like the create() for the bundle, but this one also copies
        the dataset and makes and entry for the partition '''
        
        if super(PartitionDb, self).create():
        
            # Copy the dataset record
            bdbs = self.bundle.database.session 
            s = self.session
            dataset = bdbs.query(Dataset).one()
            s.merge(dataset)
            s.commit()
            
            # Copy the partition record
            from databundles.orm import Partition as OrmPartition 
        
            orm_p = bdbs.query(OrmPartition).filter(
                            OrmPartition.id_ == self.partition.identity.id_).one()
            s.merge(orm_p)
          
            #Copy the tables and columns
            if copy_tables:
                if orm_p.t_id is not None:
                    table = bdbs.query(Table).filter(Table.id_ == orm_p.t_id).one()
                    s.merge(table)
                    for column in table.columns:
                        s.merge(column)
                else:
                    for table in dataset.tables:
                        s.merge(table)
                        for column in table.columns:
                            s.merge(column)
                
            s.commit()
                  
            # Create a config key to mark this as a partition
 