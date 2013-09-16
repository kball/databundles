"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse #@UnresolvedImport
from ..library import LibraryDb


class PostgresWarehouse(RelationalWarehouse):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, progress_cb=None):
        super(PostgresWarehouse, self).__init__(database,  library=library, storage=storage, 
                                                resolver = resolver, progress_cb=progress_cb)
        
    def _copy_command(self, table, url):
        
        template = """COPY "public"."{table}" 
FROM  PROGRAM 'curl -s -L --compressed "{url}" | tail -n +2' 
WITH ( DELIMITER '|', NULL '' ) ;"""

        return template.format(table = table, url = url)
     
    def _install_partition(self, bundle, partition):
        from multiprocessing.pool import ThreadPool as Pool
        
        self.progress_cb('install_partition',partition.identity.name,None)

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        s = self.database.session
        # Create the tables
        
        for table_name in tables:
            table, meta = self.create_table(partition.identity.as_dataset.vid, table_name, use_id = True)
            break; # This code actually only allows one table. 
        
        self.database.session.commit()
            
        # Look for the segmented CSV files coresponding to the Sqlite partition
        ident = partition.identity    
        ident.format = 'csv'
        ident.segment = partition.identity.ANY

        pool = Pool(4)

        pool.map(lambda p: self._install_csv_partition(table, p),  bundle.partitions.find_all(ident))

    def _install_csv_partition(self, table, p):
            self.progress_cb('install_partition',p.identity.name,None)
            cmd =  self._copy_command(table.name, self.resolver.url(p.identity.vid))

            self.database.session.execute(cmd)
            self.database.session.commit()
            
            self.progress_cb('installed_partition',p.identity.name,None)       
            
            
            