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
        

    def _install_partition(self, partition):

        self.progress_cb('install_partition',partition.identity.name,None)

        pdb = partition.database
     
        tables = partition.data.get('tables',[])

        s = self.database.session
        # Create the tables
        for table_name in tables:
            self.create_table(partition.identity.as_dataset.vid, table_name, use_id = True)
            self.progress_cb('create_table',table_name,None)
        
        self.database.session.commit()
            