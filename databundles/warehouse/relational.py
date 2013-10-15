"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from . import WarehouseInterface

class RelationalWarehouse(WarehouseInterface):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, logger=None):

        super(RelationalWarehouse, self).__init__(database,  library=library, storage=storage, 
                                                  resolver = resolver, logger=logger)
        
        self.library.database.create()
        
    def __del__(self):
        pass # print self.id, 'closing Warehouse'

    def get(self, name_or_id):
        """Return true if the warehouse already has the referenced bundle or partition"""
        
        r = self.library.database.get_name(name_or_id)

        if not r[0]:
            r = self.library.database.get_id(name_or_id)
        
        return r
        
    def has(self, name_or_id):
        
        dataset, partition = self.get(name_or_id)

        return bool(dataset)
        
    def install_dependency(self, name):
        '''Install a base dependency, from its name '''
        if not self.resolver:
            raise Exception("Can't resolve a dependency without a resolver defined")

        self.log('get_dependency {}'.format(name))

        b = self.resolver(name)
        
        if not b:
            raise DependencyError("Resolver failed to get {}".format(name))
        
        self.logger.log('install_dependency '+name)

        self.install(b)
      
    def install_by_name(self,name):
    
        self.logger.log('install_name {}'.format(name))

        d, p = self.resolver.get_ref(name)
        
        if not d:
            raise DependencyError("Resolver failed to get dataset reference for {}".format(name))
        
        if not p:
            raise ValueError("Name must refer to a partition")
        
        if not self.has(d.vid):
            self.logger.log('install_dataset {}'.format(name))
            
            b = self.resolver.get(d.vid)
            
            if not b:
                raise DependencyError("Resolver failed to get dataset for {}".format(d.vname))
                  
            self._install_bundle(b)
        else:
            self.logger.log('dataset already installed {}'.format(d.vname))
        
        if p:
            b = self.resolver.get(d.vid)
            self.install_partition_by_name(b, p)
        
    
    def install_partition_by_name(self, bundle, p):
        
        self.logger.log('install_partition '.format(p.vname))
        
        partition = bundle.partitions.partition(p.id_)
    
        if partition.record.format == 'geo':
            self._install_geo_partition(bundle,  partition)
            
        elif partition.record.format == 'hdf':
            self._install_hdf_partition(bundle,  partition)
            
        else:
            self._install_partition(bundle, partition)


    def _install_bundle(self, bundle):
        
        self.logger.log('install_bundle {}'.format(bundle.identity.vname))
        self.library.database.install_bundle(bundle)
    
    def has_table(self, table_name):

        return table_name in self.database.inspector.get_table_names()
    
    def table_meta(self, d_vid, table_name,use_id=True):
        from ..schema import Schema

        meta, table = Schema.get_table_meta_from_db(self.library.database, table_name, d_vid = d_vid,  use_id=use_id, 
                                                    session=self.library.database.session)        
    
        return meta, table
    
    def create_table(self, d_vid, table_name, use_id = True):
        
        from ..schema import Schema

        meta, table = self.table_meta(d_vid, table_name, use_id)

        if not self.has_table(table.name):
            table.create(bind=self.database.engine)
            self.logger.log('create_table {}'.format(table.name))
        else:
            self.logger.log('table_exists {}'.format(table.name))

        return table, meta
        
    
    def _install_partition(self, partition):

        self.logger.log('install_partition {}'.format(partition.identity.name))

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        s = self.database.session
        
        # Create the tables
        for table_name in tables:
            if not table_name in self.database.inspector.get_table_names():    
                t_meta, table = partition.bundle.schema.get_table_meta(table_name, use_id=True, driver = self.database.driver) #@UnusedVariable
                table.create(bind=self.database.engine)   
                self.logger.log('create_table {}'.format(table_name))
        
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

            self.logger.log('populate_table {}'.format(table_name))
            with self.database.inserter(dest_table.name, cache_size = cache_size, caster = caster) as ins:
   
                try: self.database.session.execute("DELETE FROM {}".format(dest_table.name))
                except: pass
   
                for i,row in enumerate(pdb.session.execute(src_table.select())):
                    self.logger.progress('add_row',table_name,i)
  
                    ins.insert(row)

        self.database.session.commit()
        self.logger.log('done {}'.format(table_name))

    def _install_geo_partition(self, partition):
        #
        # Use ogr2ogr to copy. 
        #
        print "GEO Partition ", partition.database.path   
        
    
    def _install_hdf_partition(self, partition):
        
        print "HDF Partition ", partition.database.path   
          
    def remove_by_name(self,name):
        from ..orm import Dataset
        from ..bundle import LibraryDbBundle
        from sqlalchemy.exc import  NoSuchTableError, ProgrammingError
        
        dataset, partition = self.get(name)

        if partition:
            b = LibraryDbBundle(self.library.database, dataset.vid)
            p = b.partitions.find(partition)
            self.logger.log("Dropping tables in partition {}".format(p.identity.vname))
            for table_name in p.tables: # Table name without the id prefix
                
                meta, table = self.table_meta(dataset.vid, table_name, use_id=True) # May have the id_prefix
                
                try:
                    self.database.drop_table(table.name)
                    self.logger.log("Dropped table: {}".format(table.name))
                except NoSuchTableError, ProgrammingError:
                    self.logger.log("Table does not exist: {}".format(table.name))
            
            self.library.database.remove_partition(partition)
        elif dataset:
            
            b = LibraryDbBundle(self.library.database, dataset.vid)
            for p in b.partitions:
                self.remove_by_name(p.identity.vname)
            
            self.logger.log('Removing bundle {}'.format(dataset.vname))
            self.library.database.remove_bundle(dataset)
        else:
            self.logger.error("Failed to find partition or bundle by name '{}'".format(name))
        
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
     
 