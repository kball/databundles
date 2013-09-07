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
        
        r = self.database.get_name(name_or_id)

        if not r[0]:
            r = self.database.get_id(name_or_id)
        
        return r
        
    def has(self, name_or_id):
        
        dataset, partition = self.get(name_or_id)

        return bool(dataset)
        
    def install_dependency(self, name, progress_cb=None):
        '''Install a base dependency, from its name '''
        if not self.resolver:
            raise Exception("Can't resolve a dependency without a resolver defined")

        progress_cb('get_dependency',name,None)

        b = self.resolver(name)
        
        if not b:
            raise DependencyError("Resolver failed to get {}".format(name))
        
        progress_cb('install_dependency',name,None)

        self.install(b, progress_cb)
      
    def install_by_name(self,name, progress_cb=None):
    
        progress_cb('install_name',name,None)
        
        progress_cb('fetch',name,None)

        b_or_p = self.resolver(name)
        
        return self.install(b_or_p, progress_cb)
        
    
    def install(self, b_or_p, progress_cb=None):
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        progress_cb('install',b_or_p.identity.vname,None)

        if isinstance(b_or_p, Bundle):
            self._install_bundle( b_or_p, progress_cb)
            
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

        
    def _install_bundle(self, bundle, progress_cb=None):
        
        progress_cb('install_bundle',bundle.identity.vname,None)
        self.database.install_bundle(bundle)
    
    def _install_partition(self, partition, progress_cb=None):

        progress_cb('install_partition',partition.identity.name,None)

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        if not progress_cb:
            def progress_cb(type,name, n): pass

        # Create the tables
        for table_name in tables:
            if not table_name in self.database.inspector.get_table_names():    
                t_meta, table = partition.bundle.schema.get_table_meta(table_name, use_id=True, driver = self.database.driver) #@UnusedVariable
                t_meta.create_all(bind=self.database.engine)   
                progress_cb('create_table',table_name,None)
        
        self.database.session.commit()
        
        if self.database.driver == 'mysql':
            cache_size = 5000
        elif self.database.driver == 'postgres':
            cache_size = 20000
        else:
            cache_size = 50000
       
        
        for table_name in tables:
            
            
            _, dest_table = partition.bundle.schema.get_table_meta(table_name, use_id=True)
            _, src_table = partition.bundle.schema.get_table_meta(table_name, use_id=False)
            
            caster = partition.bundle.schema.table(table_name).cast_transform()

            progress_cb('populate_table',table_name,None)
            with self.database.inserter(dest_table.name, cache_size = cache_size, caster = caster) as ins:
   
                self.database.execute("DELETE FROM {}".format(dest_table.name))
   
                for i,row in enumerate(pdb.session.execute(src_table.select())):
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
        
    def exists(self):
        self.database.exists()
        
    def info(self):
        config = self.config.to_dict()

        if 'password' in config['database']: del config['database']['password']
        return config
     
 