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