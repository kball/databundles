from ..dbexceptions import ConfigurationError

def new_warehouse(config):

    type_ = config['database']['driver']

    if type == 'bigquery':
        pass
    else:
        from relational import RelationalWarehouse #@UnresolvedImport
        return RelationalWarehouse(config)
        
    
class WarehouseInterface:
    
    
    def __init__(self, config,  resolver_cb = None):
        pass