
from __future__ import absolute_import
from ..dbexceptions import ConfigurationError
from ..library import LibraryDb
from ..cache import new_cache
from ..database import new_database

def new_warehouse(config):

    service = config['service'] if 'service' in config else 'relational'
    
    database = new_database(config['database'],'warehouse')
    storage = new_cache(config['storage']) if 'storage' in config else None
    library = LibraryDb(**config['library']) if 'library' in config else  LibraryDb(**config['database'])

    if service == 'bigquery':
        pass
    elif service == 'redshift':
        from .redshift import RedshiftWarehouse  #@UnresolvedImport

        return RedshiftWarehouse(database,storage=storage, library=library)
    
    elif service == 'postgres':
        from .postgres import PostgresWarehouse  #@UnresolvedImport

        return PostgresWarehouse(database=database,storage=storage, library=library)
    else:
        from .relational import RelationalWarehouse #@UnresolvedImport
        return RelationalWarehouse(database,storage=storage, library=library)
        
    
class WarehouseInterface(object):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, progress_cb=None):
        
        self.database = database
        self.storage = storage
        self.library = library
        self.resolver = resolver if resolver else lambda name: False
        self.progress_cb = progress_cb if progress_cb else lambda type,name,n: True

        if not self.library:
            self.library = self.database
            
            