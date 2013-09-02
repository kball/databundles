"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import DatabaseInterface
import anydbm
import os

class Dbm(DatabaseInterface):
    
    def __init__(self, bundle, db, table=None, suffix=None):

        self.bundle = bundle

        self.suffix = suffix

        self._table = table
        try:
            table_name = table.name
        except:
            table_name = table

        self._path = str(db.path)

        if table_name:
            self._path += '-'+table_name
            
        if suffix:
            self._path += '-'+suffix
            
        self._path += '.dbm'
       
            
        self._file = None
      
        
    @property
    def reader(self):
        self.close()
        self._file = anydbm.open(self._path, 'r')
        return self
   
    @property
    def writer(self):
        self.close()
        self._file = anydbm.open(self._path, 'c')
        return self
        
    def delete(self):
        
        if os.path.exists(self._path):
            os.remove(self._path)
        
        
    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    
    def __getitem__(self, key):
        return self._file[key]
        

    def __setitem__(self, key, val):
        #print key,'<-',val
        self._file[str(key)] =  str(val)
    

      