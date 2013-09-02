
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from . import DatabaseInterface
import anydbm
import os


class TempFile(object): 
           
    def __init__(self, bundle,  db, table, suffix=None, header=None, ignore_first=False):
        self.bundle = bundle
        self.db = db 
        self.table = table
        self.file = None
        self.suffix = suffix
        self.ignore_first = ignore_first

        if header is None:
            header = [ c.name for c in table.columns ]
        else:
            pass

        self.header = header

        name = table.name
        
        if suffix:
            name += "-"+suffix

        self._path = str(self.db.path)+'.d/'+name+".csv"
        
        self._writer = None
        self._reader = None
        
    def __enter__(self): 
        return self
        
    def insert(self, row):
        self.writer.writerow(row)
        
        
    @property
    def writer(self):
        if self._writer is None:
            import csv
            self.close()
            
            if self.exists:
                mode = 'a+'

            else:
                mode = 'w'
                try: os.makedirs(os.path.dirname(self.path))
                except: pass

            self.file = open(self.path, mode)
            self._writer = csv.writer(self.file)
            
            if mode == 'w':
                if self.ignore_first:
                    self._writer.writerow(self.header[1:])
                else:
                    self._writer.writerow(self.header)
                
        return self._writer
     
    @property
    def linewriter(self):
        '''Like writer, but does not write a header. '''
        if self._writer is None:
            import csv
            self.close()
            
            if self.exists:
                mode = 'a+'
            else:
                mode = 'w'
            
            if not os.path.exists(self.path):
                if not os.path.exists(os.path.dirname(self.path)):
                    os.makedirs(os.path.dirname(self.path))
            
            self.file = open(self.path, mode)
            self._writer = csv.writer(self.file)

        return self._writer
            
  
    @property
    def reader(self, mode='r'):
        '''Open a DictReader on the temp file. '''
        if self._reader is None:
            import csv
            self.close()
            self.file = open(self.path, mode, buffering=1*1024*1024)
            self._reader = csv.DictReader(self.file)
            
        return self._reader
       
    @property
    def linereader(self, mode='r', skip_header = True):
        '''Open a regular, list-oriented reader on the temp file
        '''
        if self._reader is None:
            import csv
            self.close()
            self.file = open(self.path, mode, buffering=1*1024*1024)
            self._reader = csv.reader(self.file)
            
        return self._reader
       
    @property 
    def path(self):
        return self._path

    @property
    def exists(self):
        return os.path.exists(self.path)
    
    def delete(self):
        self.close()
        if self.exists:
            os.remove(self.path)
    
    def close(self):
        if self.file:
            self.file.flush()
            self.file.close()
            self.file = None
            self._writer = None
            
            hk = self.table.name+'-'+str(self.suffix)
            if hk in self.db._tempfiles:
                del self.db._tempfiles[hk]
  
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self
