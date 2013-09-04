"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlite import Database #@UnresolvedImport


class InstalledBundleDb(Database): # Previously BundleDB
    
    '''Represents the database version of a bundle that is installed in a library'''
    def __init__(self, bundle, path):
        import os
        super(InstalledBundleDb, self).__init__(bundle, path)  
        
        self.base_path, _ = os.path.splitext(path)

    @property 
    def path(self):
        return self.base_path + self.EXTENSION
    
    def sub_dir(self, *args):
        return  self.bundle.sub_dir(*args)
    
class BuildBundleDb(Database):

    @property 
    def path(self):
        return self.bundle.path + self.EXTENSION
    
    def sub_dir(self, *args):
        return  self.bundle.sub_dir(*args)