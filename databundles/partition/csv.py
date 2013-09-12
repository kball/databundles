"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

  
from . import PartitionBase, PartitionIdentity
from ..database.csv import CsvDb
   
class CsvPartitionIdentity(PartitionIdentity):
    PATH_EXTENSION = '.csv'
    

        
class CsvPartition(PartitionBase):
    ''' '''
    
    FORMAT = 'csv'    
    
    def __init__(self, bundle, record):
        super(CsvPartition, self).__init__(bundle, record)

        self._db_class = CsvDb


    @property
    def database(self):

        if self._database is None:
            self._database = CsvDb(self.bundle, self, self.path)
          
        return self._database
    
    def create(self):
        self.database.create()

    def write_stats(self):
        '''Assumes the partition is written without a header and that the 
        first column is the id. '''
        
        t = self.table
        
        if not t:
            return


        count = 0
        min_ = 2**63
        max_ = -2**63
        
        for row in self.database.reader():
            count += 1
            v = int(row[0])
            min_ = min(min_, v)
            max_ = max(max_, v)


        self.record.count = count
        self.record.min_key = min_
        self.record.max_key = max_
     
        bs = self.bundle.database.session
        bs.merge(self.record)
        bs.commit()

    def __repr__(self):
        return "<csv partition: {}>".format(self.name)