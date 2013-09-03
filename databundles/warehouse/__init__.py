

from ..dbexceptions import ConfigurationError
from databundles.util import lru_cache
from . import Warehouse

@lru_cache(maxsize=128)
def new_warehouse(config):

    return Warehouse(database, config)

class WarehouseInterface:
    
    
    def __init__(self, config,  resolver_cb = None):
        pass