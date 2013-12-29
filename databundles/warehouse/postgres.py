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
       
       
    def drop_user(self, u):
        e = self.database.connection.execute
        
        e("DROP SCHEMA {} CASCADE;".format(u))
        e("DROP OWNED BY {}".format(u))
        e("DROP ROLE {}".format(u))  
              
    def create_user(self, u):
        
        e = self.database.connection.execute
        
        e("CREATE ROLE {0} LOGIN PASSWORD '{0}'".format(u))
        
        e("CREATE SCHEMA {0} AUTHORIZATION {0};".format(u))
        
        e("ALTER ROLE {0} SET search_path TO library,public,{};".format(u))
        
        # From http://stackoverflow.com/a/8247052
        e("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}".format(u))
        e("""ALTER DEFAULT PRIVILEGES IN SCHEMA public 
             GRANT SELECT ON TABLES  TO {}; """.format(u))

        e("GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO {}".format(u))
        e("""ALTER DEFAULT PRIVILEGES IN SCHEMA public 
          GRANT SELECT, USAGE ON SEQUENCES  TO {}""".format(u))
        
    def users(self):
        
        q = """SELECT 
            u.usename AS "name", 
            u.usesysid AS "id",
            u.usecreatedb AS "createdb",
            u.usesuper AS "superuser"
            FROM pg_catalog.pg_user u
            ORDER BY 1;"""
        
        return { row['name']:dict(row) for row 
                in self.database.connection.execute(q) }

    def _copy_command(self, table, url):
        
        template = """COPY "public"."{table}"  FROM  PROGRAM 'curl -s -L --compressed "{url}"'  WITH ( FORMAT csv )"""

        return template.format(table = table, url = url)
     
    def _install_partition(self, partition):

        from databundles.client.exceptions import NotFound
        
        self.logger.log('install_partition_csv {}'.format(partition.identity.name))

        pdb = partition.database

        for table_name in partition.data.get('tables',[]):
            sqla_table, meta = self.create_table(partition.identity, table_name)
            orm_table = partition.get_table(table_name)
            
            try:
                urls = self.resolver.csv_parts(partition.identity.vid, orm_table.id_)
            except NotFound:
                self.logger.error("install_partition {} CSV install failed because partition was not found on remote server"
                                  .format(partition.identity.name))
                raise 

            for url in urls:
                self._install_csv_url(sqla_table, url)

            self.library.database.install_table(orm_table.vid, sqla_table.name)
        
    def _install_csv_url(self, table, url):
        
        self.logger.log('install_csv_url {}'.format(url))

        cmd =  self._copy_command(table.name, url)
        self.logger.log('installing with command: {} '.format(cmd))
        r = self.database.connection.execute(cmd)
                
        #self.logger.log('installed_csv_url {}'.format(url)) 
        
        r = self.database.connection.execute('commit')

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
        
            