"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from . import WarehouseInterface

class RelationalWarehouse(WarehouseInterface):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, progress_cb=None):

        super(RelationalWarehouse, self).__init__(database,  library=library, storage=storage, 
                                                  resolver = resolver, progress_cb=progress_cb)
        
        self.library.create()
        
    def __del__(self):
        pass # print self.id, 'closing Warehouse'

    def get(self, name_or_id):
        """Return true if the warehouse already has the referenced bundle or partition"""
        
        r = self.library.get_name(name_or_id)

        if not r[0]:
            r = self.library.get_id(name_or_id)
        
        return r
        
    def has(self, name_or_id):
        
        dataset, partition = self.get(name_or_id)

        return bool(dataset)
        
    def install_dependency(self, name):
        '''Install a base dependency, from its name '''
        if not self.resolver:
            raise Exception("Can't resolve a dependency without a resolver defined")

        self.progress_cb('get_dependency',name,None)

        b = self.resolver(name)
        
        if not b:
            raise DependencyError("Resolver failed to get {}".format(name))
        
        self.progress_cb('install_dependency',name,None)

        self.install(b)
      
    def install_by_name(self,name):
    
        self.progress_cb('install_name',name,None)
        
        self.progress_cb('fetch',name,None)


        return 

        b_or_p = self.resolver(name)
        
        return self.install(b_or_p)
        
    
    def install(self, b_or_p):
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        self.progress_cb('install',b_or_p.identity.vname,None)

        if isinstance(b_or_p, Bundle):
            self._install_bundle( b_or_p)
            
        elif isinstance(b_or_p, PartitionInterface):
            
            if not self.has(b_or_p.bundle.identity.vname):
                self.install_dependency(b_or_p.bundle.identity.vname)

            if b_or_p.record.format == 'geo':
                self._install_geo_partition( b_or_p)
                
            elif b_or_p.record.format == 'hdf':
                self._install_hdf_partition( b_or_p)
            else:
                self._install_partition( b_or_p)
        else:
            raise ValueError("Can only install a partition or bundle")
        
    def _install_bundle(self, bundle):
        
        self.progress_cb('install_bundle',bundle.identity.vname,None)
        self.library.install_bundle(bundle)
    
    def has_table(self, table_name):
        pass
    
    def create_table(self, d_vid, table_name, use_id = True):
        
        from ..schema import Schema

        meta, table = Schema.get_table_meta_from_db(self.library, table_name, d_vid = d_vid,  use_id=use_id)

        table.create(bind=self.database.engine)

        return table, meta
        
    
    def _install_partition(self, partition):

        self.progress_cb('install_partition',partition.identity.name,None)

        pdb = partition.database
     
        tables = partition.data.get('tables',[])


        s = self.database.session
        # Create the tables
        for table_name in tables:
            if not table_name in self.database.inspector.get_table_names():    
                t_meta, table = partition.bundle.schema.get_table_meta(table_name, use_id=True, driver = self.database.driver) #@UnusedVariable
                table.create(bind=self.database.engine)   
                self.progress_cb('create_table',table_name,None)
        
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

            self.progress_cb('populate_table',table_name,None)
            with self.database.inserter(dest_table.name, cache_size = cache_size, caster = caster) as ins:
   
                try: self.database.session.execute("DELETE FROM {}".format(dest_table.name))
                except: pass
   
                for i,row in enumerate(pdb.session.execute(src_table.select())):
                    self.progress_cb('add_row',table_name,i)
  
                    ins.insert(row)

        self.database.session.commit()
        self.progress_cb('done',table_name,None)

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
     
 