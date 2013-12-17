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
    
    def create(self):
        self.database.create()
        self.database.connection.execute('CREATE SCHEMA IF NOT EXISTS library;')
        self.library.database.create()
        
    def _copy_command(self, table, url):
        
        template = """COPY "public"."{table}"  FROM  PROGRAM 'curl -s -L --compressed "{url}"'  WITH ( DELIMITER '|', NULL '' )"""

        return template.format(table = table, url = url)
     
    def _install_partition(self, partition):

        from databundles.client.exceptions import NotFound
        
        self.logger.log('install_partition {}'.format(partition.identity.name))

        pdb = partition.database

        for table_name in partition.data.get('tables',[]):
            sqla_table, meta = self.create_table(partition.identity, table_name)

            try:
                urls = self.resolver.csv_parts(partition.identity.vid)
            except NotFound:
                self.logger.error("install_partition {} CSV install failed because partition was not found on remote server"
                                  .format(partition.identity.name))
                raise 
         
            for url in urls:
                self._install_csv_url(sqla_table, url)
                
            orm_table = partition.get_table()
            
            self.library.database.install_table(orm_table.vid, sqla_table.name)
        
    def _install_csv_url(self, table, url):
        
        self.logger.log('install_csv_url {}'.format(url))

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

        self.logger.log('installed_csv_url {}'.format(url)) 
        
        
    def remove_by_name(self,name):
        '''Call the parent, then remove CSV partitions'''
        from ..bundle import LibraryDbBundle
        
        super(PostgresWarehouse, self).remove_by_name(name)

        dataset, partition = self.get(name)

        if partition:
            b = LibraryDbBundle(self.library.database, dataset.vid)
            p = b.partitions.find(partition)
 
            for p in p.get_csv_parts():
                super(PostgresWarehouse, self).remove_by_name(p.vname)
        
            