"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


def new_database(config):
    pass

class DatabaseInterface(object):
    
    @property
    def name(self):  
        raise NotImplementedError() 
    
    @property 
    def path(self):
        raise NotImplementedError() 
   
    def exists(self):
        raise NotImplementedError() 
    
    def create(self):
        raise NotImplementedError() 
    
    def add_post_create(self, f):
        raise NotImplementedError() 
    
    def delete(self):
        raise NotImplementedError() 
    
    def open(self):
        raise NotImplementedError() 
    
    def close(self):
        raise NotImplementedError() 
    
    def inserter(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def updater(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def commit(self):
        raise NotImplementedError() 
   
