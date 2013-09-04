"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from . import WarehouseInterface
from ..library import LibraryDb


class RelationalWarehouse(WarehouseInterface):
    
    def __init__(self, config,  resolver_cb = None):
        self.config = config
    
        self.resolver = resolver_cb # For fetching dependencies. 
        
        self.database = LibraryDb(**config['database'])
        
    def __del__(self):
        pass # print self.id, 'closing Warehouse'
        

        
    
    def get(self, name_or_id):
        """Return true if the warehouse already has the referenced bundle or partition"""
        
        r = self.database.get_id(name_or_id)
        
        if not r:
            r = self.database.get_name(name_or_id)
        
        return r
        
    def has(self, name_or_id):
        dataset, partition = self.get(name_or_id)
        
        return bool(dataset)
        
    def install_dependency(self, name, progress_cb=None):
        
        if not self.resolver:
            raise Exception("Can't resolve a dependency without a resolver defined")

        b = self.resolver(name)
        
        if not b:
            raise DependencyError("Resolver failed to get {}".format(name))
        
        
        self.install(b, progress_cb)
      
    
    def install(self, b_or_p, progress_cb=None):
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        if isinstance(b_or_p, Bundle):
            self._install_bundle( b_or_p)
            
        elif isinstance(b_or_p, PartitionInterface):
            
            if not self.has(b_or_p.bundle.identity.vname):
                self.install_dependency(b_or_p.bundle.identity.vname, progress_cb)

            if b_or_p.record.format == 'geo':
                self._install_geo_partition( b_or_p)
                
            elif b_or_p.record.format == 'hdf':
                self._install_hdf_partition( b_or_p)
            else:
                self._install_partition( b_or_p, progress_cb)
        else:
            raise ValueError("Can only install a partition or bundle")

        
    def _install_bundle(self, bundle):
        
        self.database.install_bundle(bundle)
    
    def _install_partition(self, partition, progress_cb=None):
        
        print "Contemplating ", partition.database.path    

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        if not progress_cb:
            def progress_cb(type,name, n): pass

        # Create the tables
        for table_name in tables:
            if not table_name in self.database.inspector.get_table_names():    
                t_meta, table = partition.bundle.schema.get_table_meta(table_name, use_id=True) #@UnusedVariable
                t_meta.create_all(bind=self.database.engine)   
                progress_cb('create_table',table_name,None)
        
        self.database.session.commit()
        
        for table_name in tables:
            
            dest_t_meta, dest_table = partition.bundle.schema.get_table_meta(table_name, use_id=True)
            src_t_meta, src_table = partition.bundle.schema.get_table_meta(table_name, use_id=False)

            cache = []
            cache_size = 100
            progress_cb('populate_table',table_name,None)
            with self.database.inserter(dest_table.name, replace=True) as ins:
                for i,row in enumerate(pdb.session.execute(src_table.select()).fetchall()):
                    progress_cb('add_row',table_name,i)
                    ins.insert(row)
                    
            
        self.database.session.commit()
        progress_cb('done',table_name,None)
     
    def _install_geo_partition(self, partition):
        #
        # Use ogr2ogr to copy. 
        #
        print "GEO Partition ", partition.database.path   
        
    
    def _install_hdf_partition(self, partition):
        
        print "HDF Partition ", partition.database.path   
        
 
                        
    def uninstall(self,b_or_p):
        pass
        
    def clean(self):
        self.database.clean()
        
    def drop(self):
        self.database.drop()
        
    def create(self):
        self.database.create()
        
    def info(self):
        config = dict(self.config)
        del config['password']
        return config
     
 