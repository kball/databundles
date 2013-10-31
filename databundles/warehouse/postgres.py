"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse #@UnresolvedImport
from ..library import LibraryDb


class PostgresWarehouse(RelationalWarehouse):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, logger=None):
        super(PostgresWarehouse, self).__init__(database,  library=library, storage=storage, 
                                                resolver = resolver, logger=logger)
        
    def _copy_command(self, table, url):
        
        template = """COPY "public"."{table}"  FROM  PROGRAM 'curl -s -L --compressed "{url}"'  WITH ( DELIMITER '|', NULL '' )"""

        return template.format(table = table, url = url)
     
    def _install_partition(self, bundle, partition):

        from databundles.client.exceptions import NotFound
        
        self.logger.log('install_partition {}'.format(partition.identity.name))

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        for table_name in tables:
            table, meta = self.create_table(partition.identity.as_dataset.vid, table_name, use_id = True)
            break; # This code actually only allows one table. 

        # Look for the segmented CSV files coresponding to the Sqlite partition
        ident = partition.identity    
        ident.format = 'csv'
        ident.segment = partition.identity.ANY

        try:
            for p in bundle.partitions.find_all(ident):
                self._install_csv_partition(table, p)
        except NotFound:
            self.logger.log('install_partition {} Failed'.format(partition.identity.name))

    def _install_csv_partition(self, table, p):
        import threading
        from databundles.client.exceptions import NotFound
        self.logger.log('install_partition {}'.format(p.identity.name))
        
        try:
            url = self.resolver.url(p.identity.vid)
        except NotFound:
            self.logger.error("install_partition {} CSV install failed because partition was not found on remote server".format(p.identity.name))
            raise 
     
        cmd =  self._copy_command(table.name, url)
        self.logger.log('installing with command: {} '.format(cmd))
        r = self.database.connection.execute(cmd)
        r = self.database.connection.execute('commit')
        
        try: self.logger.log("Install result (a): {}".format(r))
        except: pass
        
        try: self.logger.log("Install result (b): {}".format(r.fetchone()))
        except: pass

        try: self.logger.log("Install result (c): {}".format(r.fetchall()))
        except: pass

        self.logger.log('installed_partition {}'.format(p.identity.name)) 
            