"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class InserterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def insert(self, row, **kwargs): raise NotImplemented()
    
    def close(self): raise NotImplemented()

class UpdaterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def update(self, row): raise NotImplemented()
    
    def close(self): raise NotImplemented()

