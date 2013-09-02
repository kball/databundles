"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
   
from inserter import InserterInterface, UpdaterInterface
from .partition import PartitionDb
from ..geo.sfschema import TableShapefile

class FeatureInserter(InserterInterface):
    def __init__(self, partition, table, dest_srs=4326, source_srs=None, layer_name = None):

        self.bundle = partition.bundle
        
        self.sf = TableShapefile(self.bundle, partition.database.path, table, dest_srs, source_srs, name=layer_name)
        
    
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self
    
    def insert(self, row, source_srs=None):
        from sqlalchemy.engine.result import RowProxy
        
        if isinstance(row, RowProxy):
            row  = dict(row)
        
        return self.sf.add_feature( row, source_srs)
    
    def close(self):
        self.sf.close()
    
    @property
    def extents(self, where=None):
        '''Return the bounding box for the dataset. The partition must specify 
        a table
        
        '''
        raise NotImplemented()
        #import ..geo.util
        #return ..geo.util.extents(self.database,self.table.name, where=where)
   
    
class GeoDb(PartitionDb):
    
    def __init__(self, bundle, partition, base_path, **kwargs):
        ''''''    
        super(GeoDb, self).__init__(bundle, partition, base_path, **kwargs)  

        #self.connection.execute("SELECT load_extension('libspatialite.dylib');")

        def load_spatialite(this):
            
            pass # SHould load the spatialite library into sqlite here. 

        self.add_post_create(load_spatialite)
   
    def inserter(self,  table = None, dest_srs=4326, source_srs=None, layer_name=None):
        
        if table is None and self.partition.identity.table:
            table = self.partition.identity.table
        
        return FeatureInserter(self.partition,  table, dest_srs, source_srs, layer_name = layer_name)
