"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
   
from inserter import InserterInterface, UpdaterInterface
from .partition import PartitionDb
from ..geo.sfschema import TableShapefile

class FeatureInserter(InserterInterface):
    
    def __init__(self, partition, table, dest_srs=4326, source_srs=None, layer_name = None):

        self.partition = partition
        self.bundle = partition.bundle
        
        
        self.sf = TableShapefile(self.bundle, partition.database.path, table, dest_srs, source_srs, name=layer_name)
        
    
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        self.partition.database.post_create()
                
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

        kwargs['driver'] = 'spatialite' 

        super(GeoDb, self).__init__(bundle, partition, base_path, **kwargs)  

    @property
    def engine(self):
        return self._get_engine(_on_connect_geo)
   
   
    def inserter(self,  table = None, dest_srs=4326, source_srs=None, layer_name=None):
        
        if table is None and self.partition.identity.table:
            table = self.partition.identity.table
        
        return FeatureInserter(self.partition,  table, dest_srs, source_srs, layer_name = layer_name)
    
def _on_connect_geo(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')
    dbapi_con.execute('PRAGMA journal_mode = OFF')
    #dbapi_con.execute('PRAGMA synchronous = OFF')
    
    try:
        from ..util import RedirectStdStreams
        with RedirectStdStreams():
            # Spatialite prints its version header always, this supresses it. 
            dbapi_con.enable_load_extension(True)
            dbapi_con.execute("select load_extension('/usr/lib/libspatialite.so')")
    except:
        pass
    
