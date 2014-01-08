"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

  
from . import PartitionBase
from ..identity import PartitionIdentity
from ..database.hdf import HdfDb
   
class HdfPartitionIdentity(PartitionIdentity):
    PATH_EXTENSION = '.hdf'
    

class HdfPartition(PartitionBase):
    '''A Partition that hosts a Spatialite for geographic data'''
    
    FORMAT = 'hdf'
    
    def __init__(self, bundle, record, **kwargs):
        super(HdfPartition, self).__init__(bundle, record)

        self._db_class = HdfDb


    @property
    def database(self):

        if self._database is None:
            self._database = HdfDb(self)
          
        return self._database


    def __repr__(self):
        return "<hdf partition: {}>".format(self.name)