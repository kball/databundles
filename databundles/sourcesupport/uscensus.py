'''
Created on Jul 13, 2012

@author: eric

Base class bundle for the US Census

'''
from  databundles.bundle import BuildBundle
import os.path  
import yaml


class UsCensusBundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(UsCensusBundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.segmap_file =  self.filesystem.path(bg.segMapFile)
        self.headers_file =  self.filesystem.path(bg.headersFile)
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.rangemap_file =  self.filesystem.path(bg.rangeMapFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.geo_map =  self.filesystem.path(bg.geoMap) #Maps record_code foriegn keys in join_geo_dim
        
        self._table_id_cache = {}

        self._urls_cache = None
    
        self._geo_tables = None
        self._states = None
        self._states_dict = None
    
        self._geo_dim_locks = {} 
        
        self._fileid  = self.config.build.fileid
        self._release_id = self.config.build.release_id
    
    def configure_arg_parser(self, argv):
    
        def csv(value):
            return value.split(',')
        
        parser = super(UsCensusBundle, self).configure_arg_parser(argv)
        
        parser.add_argument('-s','--subphase', action='store', default = 'all',  help='Specify a sub-phase')
        
        parser.add_argument('-S','--states', action='store', default = ['all'],  
                             type=csv,  help='Specify a sub-phase')
        
        return parser
 
    def scrape_urls(self, suffix='_uf1'):
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls = self._scrape_urls(self.config.build.rootUrl,self.states_file, suffix)
   
        with open(self.urls_file, 'w') as f:
            yaml.dump(urls, f,indent=4, default_flow_style=False)
            
        return self.urls

    #############################################
    # Generate rows from multiple files?

       
    def get_table_by_table_id(self,table_id):  
        '''Get the table definition from the schema'''
        t = self._table_id_cache.get(table_id, False)
        
        if not t:
            t = self.schema.table(table_id)
            self._table_id_cache[table_id] = t

        return t
    
    @property
    def urls(self):

        if self._urls_cache is None:
            with open(self.urls_file, 'r') as f:
                self._urls_cache =  yaml.load(f) 
            
            # In test mode, we only use the first state, to make
            # things run faster. 
            if self.run_args.test:
                x = self._urls_cache['geos'].iteritems().next()
                self._urls_cache['geos'] = dict([x])
 
        return self._urls_cache
      
    #############
    # Writing Results to Disk        
          

class UsCensusDimBundle(UsCensusBundle):
    
    #####################################
    # Peparation
    #####################################
    
    def prepare(self):
        '''Create the prototype database'''

        if not self.database.exists():
            self.database.create()

        self.scrape_urls()
      
        self.create_geo_dim_table_schema()

        self.generate_partitions()
 
        return True    
    
    def create_hash_map_table(self,name):
        from databundles.orm import Column
        
        t = self.schema.add_table(name,data={'temp':True})

        self.schema.add_column(t, name+"_id",
                       datatype=Column.DATATYPE_INTEGER, 
                       is_primary_key =True,commit = False)

        self.schema.add_column(t, 'geoid',
                       datatype=Column.DATATYPE_INTEGER, 
                       commit = False,
                       uindexes = name+'_lrid'
                       )
        
        self.schema.add_column(t, 'logrecno',
                       datatype=Column.DATATYPE_INTEGER, 
                       commit = False,
                       uindexes = name+'_lrid'
                       )

        self.schema.add_column(t, 'hash',
                       datatype=Column.DATATYPE_INTEGER, 
                       commit = False)
        
        self.schema.add_column(t, 'rec_id',
                       datatype=Column.DATATYPE_INTEGER, 
                       commit = False)


        
        #for col_name in ['fileid','stusab','sumlev','geocomp','chariter','cisfn','logrecno']:   
        #    self.schema.add_column(t, col_name,
        #                           datatype=Column.DATATYPE_INTEGER, 
        #                           is_foreign_key =True,commit = False)

     
        # Also make a partition for the table. 
        from databundles.partition import PartitionIdentity

        partition = self.partitions.find_or_new(PartitionIdentity(self.identity, table=name))
        partition.data['temp'] = True
        partition.data['map'] = True

        self.database.session.commit()

    def create_geo_dim_table_schema(self):
        '''Create the split table schema from  the geoschema_filef. 
        
        The "split" tables are the individual tables, which are split out from 
        the segment files. 
        '''

        if len(self.schema.tables) > 0 and len(self.schema.columns) > 0:
            self.log("Reusing schema")
            return True

        from databundles.orm import Column
        with open(self.geoschema_file, 'rbU') as f:
            self.schema.schema_from_file(f)
    
        # Add extra fields to all of the split_tables
        record_code_table = None
        for table in self.schema.tables:
            
            if not table.data.get('split_table', False):
                continue;
            
            # For each of the other geo dim tables, add a foreign key 
            # to the record_code
            if table.name == 'record_code':
                # Should be the first table. 
                record_code_table = table
            else:
                record_code_table.add_column(table.name+"_id",
                                             is_foreign_key =True,
                                             datatype=Column.DATATYPE_INTEGER64)
                
                # Also create another table that we will use to map the geo dim hash
                # values back to sequentual numbers. 
                self.create_hash_map_table(table.name+"_map")
            
            # Add a hash column to store a hash value for all of the other values. 
            # This is used in dicts in memory. 
            #if not table.column('hash', False):
            #    table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
            #                      uindexes = 'uihash')


    def generate_partitions(self):
        from databundles.partition import PartitionIdentity
        #
        # Geo split files
        for table in self.geo_tables:
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
                
                
        # The map files link the hash for a split table to the sequential id for that
        # table. 

    @property
    def states(self):
        if self._states  is None:
            if 'all' in self.run_args.states:
                states = self.urls['geos'].keys()
                states.sort()  
                self._states = states
            else:
    
                states = [ s for s in self.urls['geos'].keys() if s in self.run_args.states ]
                states.sort()
                self._states = states 
                 
        return self._states

    def make_range_map(self):
        
        if os.path.exists(self.rangemap_file):
            self.log("Re-using range map")
            return

        self.log("Making range map")

        range_map = {}
        
        segment = None
       
        for table in self.schema.tables:
            
            if table.data.get("split_table", False) or table.name == 'sf1geo':
                # Don't look at geo dim tables
                continue;
   
            if segment != int(table.data['segment']):
                last_col = 4
                segment = int(table.data['segment'])
            
            col_start = min(int(c.data['source_col']) for c in table.columns if c.data.get('source_col', False))
            col_end = max(int(c.data['source_col']) for c in table.columns if c.data.get('source_col', False))
        
            if segment not in range_map:
                range_map[segment] = {}
        
            range_map[segment][table.id_.encode('ascii', 'ignore')] = {
                                'start':last_col + col_start,  
                                'end':last_col + col_end+ 1, 
                                'length': col_end-col_start + 1,
                                'table' : table.name.encode('ascii', 'ignore')}
            
                         
            #print "{:5s} {:4d} {:4d} {:4d} {:4d}".format(table.name,  int(segment), col_end-col_start + 1, 
            #                                        last_col + col_start, last_col + col_end  )

            #print range_map[segment][table.id_.encode('ascii', 'ignore')]
         
            last_col += col_end
            
            self.ptick('.')

    
        self.ptick('\n')

        with open(self.rangemap_file, 'w')as f:
            yaml.dump(range_map, f,indent=4, default_flow_style=False)  


        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        self.log("Install bundle")
        self.library.put(self)
       

    #############################################
    # Build 
    #############################################
    
    def build(self, run_join_geo_dim_f = None, run_geo_dim_f=None, run_state_tables_f=None,run_fact_db_f=None):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        from multiprocessing import Pool

        if self.run_args.subphase in ['test']:
            print self.states
            print self.states_dict
         
        # Special process to run the sf1geo partition
        if self.run_args.subphase in ['sf1geo']:
            self.run_sf1_geo()
         
        # Split up the state geo files into .csv files, and 
        # create the build/geodim files that will link logrecnos to
        # geo split table records. 
        if self.run_args.subphase in ['all','geo-dim']:
    
            if self.run_args.multi and run_geo_dim_f:
                
                pool = Pool(processes=int(self.run_args.multi))
          
                result = pool.map_async(run_geo_dim_f, enumerate(self.urls['geos'].keys()))
                print result.get()
            else:
                for state in self.states:
                    self.run_geo_dim(state)
        
        # Now we have all of the geo data broken down into seperate .csv files, 
        # one per geo table and state. 
     
        if self.run_args.subphase in ['all','join-geo-dim']:   
            
            if self.run_args.multi and run_join_geo_dim_f:
                
                pool = Pool(processes=int(self.run_args.multi))

                ids = [p.identity.id_ for p in self.geo_partition_map().values()]

                result = pool.map_async(run_join_geo_dim_f,ids)
                print result.get()
            else:
                for table_id, partition in  self.geo_partition_map().items(): #@UnusedVariable
                    self.join_geo_dim(partition)

        if self.run_args.subphase in ['all','join-maps']:  
            self.join_maps()

        if self.run_args.subphase in ['all','reindex']:  
            self.reindex()

        return False

        # Join all of the seperate partitions into a single partition
        
        if self.run_args.subphase in ['all','join-partitions']:  
            self.join_partitions()

        if self.run_args.subphase in ['all','load-sql']:  
            self.post_create_file =  self.filesystem.path(self.config.build.postCreateSql)
            self.database.load_sql(self.post_create_file)

        return True

    def run_sf1_geo(self):
        import time
        from databundles.partition import PartitionIdentity
        
        t_start = time.time()
        row_i = 0
        
        table = self.schema.table('sf1geo')
        
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            self.log("Create partition for "+table.name)
            partition = self.partitions.new_partition(pid)
        
        if not partition.database.exists():
            partition.create_with_tables(table.name)
        
        partition.database.connection.execute("delete from sf1geo")
        
        # Make the sqlite connect allow 8 bit strings, which are used in 
        # the names in Puerto Rico

        with partition.database.inserter(partition.table) as ins:
            for state in self.states:
                for row in self.generate_rows(state): #@UnusedVariable
                    
                    row_i += 1
                
                    if row_i % 100000 == 0:
                        # Prints the processing rate in 1,000 records per sec.
                        self.log("SF1 "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
                
                    #print row['fileid'], row['stusab'], row['sumlev'], row['geocomp'], row['chariter'], row['cifsn'], row['logrecno']
                    row['name'] = row['name'].decode('latin1').encode('ascii','xmlcharrefreplace')
                    ins.insert(row)


        partition.database.load_sql(self.filesystem.path(self.config.build.sf1IndexSql))

    def run_geo_dim(self, state):
        '''Break up a state geo file into seperate geo dim split tables, as CSV files. 
        This will aso create a CSV file for the record_code table for the state, which 
        holds the hash values of the split table entries. '''
        
        import time
     
        geo_partitions = self.geo_partition_map() # must come before geo_processors. Creates partitions
        geo_processors = self.geo_processors()
     
        fileid  = self.config.build.fileid

        row_hash_map = {} #@RservedAssignment
        for table_id, cp in geo_processors.items(): #@UnusedVariable
            row_hash_map[table_id] = set()
     
        row_i = 0
        
        marker_f = self.filesystem.build_path('markers',state+"_geo_dim")
        
        if os.path.exists(marker_f):
            self.log("Geo dim exists for {}, skipping".format(state))
            return
        else:
            self.log("Building geo dim for {}".format(state))
       
        record_code_partition = self.get_record_code_partition(geo_partitions)
           
        tf = record_code_partition.tempfile( suffix=state)
        if tf.exists:
            tf.delete()
    
        # Delete any of the files that may still exist. 
                    
        for table_id, cp in geo_processors.items():
            partition = geo_partitions[table_id]
            tf = partition.tempfile(suffix=state)

            if tf.exists:
                tf.delete()
                
            try:
                # ignore_first removes the first id field from the header. We only need
                # it here on the first call to tempfile, since the object is cached. 
                ptmtf = partition.table_map.tempfile(suffix=state,ignore_first=True);
                if ptmtf.exists:
                    tf.delete()
            except:
                pass

        for geo in self.generate_rows(state): #@UnusedVariable
  
            if row_i == 0:
                self.log("Starting loop for state: "+state+' ')
                t_start = time.time()
      
            geo['abbrev'] = state
      
            row_i += 1
            
            if row_i % 10000 == 0:
                # Prints the processing rate in 1,000 records per sec.
                self.log("GEO "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")


            geoid = self.make_geoid(fileid, state, geo['sumlev'], geo['geocomp'], 
                                 geo['chariter'], geo['cifsn'])
       
            # Iterate over all of the geo dimension tables, taking part of this
            # geo row and putting it into the temp file for that geo dim table. 
      
            hash_keys = []
            for table_id, cp in geo_processors.items():
                table,  columns, processors = cp #@UnusedVariable
            
                partition = geo_partitions[table_id]

                if partition == record_code_partition:
                    continue

                values=[ f(geo) for f in processors ]
                row_hash = self.row_hash(values)
                th = row_hash_map[table.id_]
                
                # The local row_hash check reduces the number of calls to writerow, but
                # since we are operating on states independently, it does not
                # guarantee uniqueness across states. 
                if row_hash not in th:  
                    th.add(row_hash)
                    values[0] = row_hash
                    
                    tf = partition.tempfile( suffix=state)
                    tf.writer.writerow(values)

                ptmtf = partition.table_map.tempfile(suffix=state);
                ptmtf.writer.writerow((geoid,  int(geo['logrecno']), row_hash, 0 ))
                
                hash_keys.append(row_hash)

            # The first None is for the primary id, the last is for the 
            # row_hash, which was added automatically to geo_dim tables.           
            # The fileid comes from the bundle.yaml configuration b/c it is the same for all records
            # in the bundle. 
         
            values = [None, int(geo['logrecno']),geoid]  + hash_keys
            tf = record_code_partition.tempfile(suffix=state)
            tf.writer.writerow(values)

        # Close all of the tempfiles. 
        for table_id, cp in geo_processors.items():
            partition = geo_partitions[table_id]
            partition.database.tempfile(partition.table, suffix=state).close()
 
        record_code_partition.database.tempfile(record_code_partition.table, suffix=state).close()  
            
        with open(marker_f, 'w') as f:
            f.write(str(time.time()))


    def join_maps(self):
        """
        Copy all of the seperate map files into a single temporary partition, where
        we can construct smaller, sequential id number to use for table keys instead of
        the much larger hash values. 
        """
        from databundles.partition import Partition, PartitionIdentity
        
        cp = self.partitions.find_or_new(PartitionIdentity(self.identity, grain='tempmap'))
            
        print "DATABSE", cp.identity.name, cp.database.path
        
        # First, join them all into partitions. 
        for partition in self.partitions:
    
            if partition.data.get('map',False):
                self.join_map(partition)
                self.copy_join_maps(partition, cp)
           
        self.database.create_table('record_code')
 
        #self.partitions.delete(cp) # deleted the partition record. 
 
        # Then join all of the seperate partitions into one. 
    
    def join_map(self,partition):
        print "Join Map",partition.identity.name
        partition.create()
        for state in self.states:
            ptmtf = partition.tempfile(suffix=state);
            print ptmtf.path
            partition.database.load_tempfile(ptmtf)

    def copy_join_maps(self, source_part, dest_part):
        """Copy the map table from its home partition into a single common 
        partition, and extract all of the unique hash values to product smaller
        sequential id values for the
        """
        dd = dest_part.database
        sname = source_part.table.name
        thname = source_part.table.name+'_htmap'
        aid = dd.attach(source_part.database.path)
                
        dd.connection.execute(
        "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, hash INTEGER);"
        .format(thname))
        
        dd.connection.execute(
        "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {}.{}"
        .format(sname, aid, sname))

        dd.detach(aid)

        dd.connection.execute("INSERT INTO {} SELECT DISTINCT NULL,hash FROM {}"
                              .format(thname, source_part.table.name))
        

        print "AID",aid
        

    def reindex(self):
        from databundles.partition import Partition, PartitionIdentity
        import tables
        
        h5 = self.database.hd5file()
        t = h5.table('record_code')
     
        
        rcp = self.get_record_code_partition()
        
        row_i = 0;
        for state in self.states:
            tf = rcp.database.tempfile(rcp.table, suffix=state)
            reader = tf.linereader
            reader.next() # skip the header. 

            for row in reader:
                row_i += 1
                row[0] = row_i
                t.append([row])
                
                if row_i > 100:
                    break;
                  
        
        h5.close()
        
        h5 = self.database.hd5file()
        t = h5.table('record_code',mode='w')
        
        for i,row in enumerate(t.iterrows()):
            print i,row.fetch_all_fields()
        
            if i> 100:
                break;
        
        h5.close()
        
        return
        
        cp = self.partitions.find_or_new(PartitionIdentity(self.identity, grain='tempmap'))
   
        hash_maps = {}
        for partition in self.geo_partition_map().values():
            table = partition.table
           
            if table.name != 'location':
                continue;

            htm = partition.table.name+'_map_htmap'

            h = {}
            for row in cp.database.connection.execute("SELECT id, hash FROM {}".format(htm)):
                h[row[0]] = row[1]
            hash_maps[table.name+"_id"] = h
            print table.name, len(h)
 
        print "Done"
        import time
        time.sleep(60)
 

    def join_partitions(self):
        '''Copy all of the seperate partitions into the main database. '''
        
        for partition in self.partitions:
        
            if partition.data.get('map',False):
                continue;
            
            if partition.table.name == 'record_code':
                continue;
        
            self.log("Join partition {}".format(partition.identity.name))
            
            if not partition.database.exists():
                self.log("   Creating partition {}".format(partition.identity.name))
                partition.create_with_tables(partition.table.name)
            
            table_name = partition.table.name
            
            self.database.create_table(table_name)    
            self.database.clean_table(table_name)
            
            attach_name = self.database.attach(partition, table_name)
            
            self.database.copy_from_attached(table_name, name=attach_name)

            self.database.detach(attach_name)

    def join_geo_dim(self, partition):
        """Assemble the partition into a the database partition, 
        and create a lookup file to be used later to set the new values for
        recno """
        import time

        t_start = time.time()
      
        row_i = 0;

        table_name = partition.table.name
        
        marker_f = self.filesystem.build_path('markers',"geo-dim-"+table_name)

        if os.path.exists(marker_f):
            self.log("Geo database marker exists for {}, skipping".format(partition.table.name))
            return partition.identity.name
        else:
            self.log("Join geo dim for {}".format(partition.table.name))
       
        hash_set = set()
        with partition.database.inserter(partition.table) as ins:
            for state in self.states:
                tf = partition.database.tempfile(partition.table, suffix=state)
                reader = tf.linereader
                reader.next() # skip the header. 
                line_no = 0

                for row in reader:
                    row_i += 1
                    line_no += 1
    
                    if row_i % 100000 == 0:
                        self.log("Join "+table_name+" "+
                             str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
                        
                    # The record_code tables doesn't use the hash for a primary lkey
                    if not row[0]: row[0] = row_i
                        
                    if row[0] not in hash_set:
                        hash_set.add(row[0])
                        ins.insert(row)
 
                tf.close()

        self.log("Hash "+table_name+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
                    
        if row_i != len(hash_set):
            self.error("{}: hash map doesn't match number of input rows: {} != {}"
                       .format(partition.table.name, len(hash_set), row_i))

        with open(marker_f, 'w') as f:
            f.write(str(time.time()))

        self.log("Joined geo dim table: {} len = {}".format(partition.table.name,len(hash_set)))
        
        return partition.identity.name

    
    @property
    def states_dict(self):
        
        if self._states_dict is None:
            self._states_dict = { state:i+1 for (i, state) in enumerate(self.states)}            
        return self._states_dict
    


    def get_record_code_partition(self, geo_partitions=None):
         
        if geo_partitions is None:
            geo_partitions = self.geo_partition_map() # must come before geo_processors. Creates partitions

        for partition in geo_partitions.values(): #@UnusedVariable
            if partition.table.name == 'record_code':
                record_code_partition = partition
                
        return record_code_partition;

    def generate_geodim_rows(self, state):
        '''Generate the rows that were created to link the geo split files with the
        segment tables'''
        
        state_no = self.states_dict[state] # convert to a number
        
        state_partition  = self.partitions.find_table('state')
        r = state_partition.database.connection.execute("select * from state where state = :state ", state=state_no).fetchone()
        print state_no, state, r
        
        rcp = self.get_record_code_partition()

        sql = "SELECT * FROM record_code, state WHERE record_code.state_id = state.state_id AND state_id = :state_id"
        
        try:
            r = rcp.database.connection.execute(sql, state_id=state_no)
        except Exception as e:
            self.error("Error in {}".format(rcp.database.path))
            raise e
            
        for row in r:
            yield row
     
        return 

        
    def geo_keys(self):
        return  [ t.name+'_id' for t in self.geo_tables]
    
    def geo_key_columns(self):
        ''' '''

        column_sets = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
         
            column_sets[table.id_] = (table, source_cols)  
       
        return column_sets
        
    def geo_partition(self, table, init=False):
        '''Called in geo_partition_map to fetch, and create, the partition for a
        table ''' 
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid.name))
        
        if init and not partition.database.exists():
            partition.create_with_tables(table.name)
            
            # Ensure that the first record is the one with all of the null values
            if False:
                # Having toubles with this causing duplicate hashes
                pass
                vals = [c.default for c in table.columns]
                vals[-1] = self.row_hash(vals)
                vals[0] = 0;
                
                ins = insert_or_ignore(table.name, table.columns)
                db = partition.database
                db.dbapi_cursor.execute(ins, vals)
                db.dbapi_connection.commit()
                db.dbapi_close()
            
        # Link in the table_map partition
        if table.name != 'record_code':
            partition.table_map = self.partitions.find_or_new(
                        PartitionIdentity(self.identity, table=table.name+"_map"))
            
        return partition
   
    def geo_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.geo_tables:
            partitions[table.id_] = self.geo_partition(table, True)
            
        return partitions

      
    #############
    # Geo Table and Partition Acessors.    
     
    def get_geo_processor(self, table):
        
        from databundles.transform import  CensusTransform
        
        source_cols = ([c.name for c in table.columns 
                            if not ( c.name.endswith('_id') and not c.is_primary_key)
                            and c.name != 'hash'
                           ])
        
        columns = [c for c in table.columns if c.name in source_cols  ]  
        processors = [CensusTransform(c) for c in columns]

        return columns, processors
            
    def geo_processors(self):
        '''Generate a complete set of geo processors for all of the split tables'''
        
        from collections import  OrderedDict
        
        processor_set = OrderedDict()
        
        for table in self.geo_tables:
            columns, processors = self.get_geo_processor(table)
            processors += [lambda row : None]
            processors[0] = lambda row : None # Primary key column
            processor_set[table.id_] = (table, columns, processors )
 
        return processor_set
        
    @property
    def geo_tables(self):
        
        if self._geo_tables is None:
      
            self._geo_tables = []
            for table in self.schema.tables:
                if table.data.get('split_table',None) == 'A':
                    self._geo_tables.append(table)
                    
        return self._geo_tables

      
    def row_hash(self, values):
        '''Calculate a hash from a database row, for geo split tables '''  
        import hashlib
        
        m = hashlib.md5()
        for x in values[1:]:  # The first record is the primary key
            try:
                m.update(x.encode('utf-8')+'|') # '|' is so 1,23,4 and 12,3,4 aren't the same  
            except:
                m.update(str(x)+'|')
        # Less than 16 to avoid integer overflow issues. Not sure it works. 
        hash = int(m.hexdigest()[:14], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
        return hash

    def make_geoid(self,  fileid, state, sumlev, geocomp, chariter, cifsn):
        """ The LRID -- Logical Record Id -- is a unique id for a logical record
        in a census file, composed of thedistinguishing identifiers for logrec lines
        and the identity of census file releases. 
        
        :param release_id:
        :param state:
        :param logrecno:

        :rtype: integer
        
        """
 
        if isinstance(state, basestring):
            try:
                # Try it as a number
                state = int(state)
            except:
                state = self.states_dict[state]
        else:
            pass

        chariter = (int(chariter) if chariter.strip()  else  -1) + 1;
        cifsn = (int(cifsn) if cifsn.strip()  else  -1) + 1

        geoid= ("{:d}{:02d}{:03d}{:02d}{:03d}{:02d}"
        .format( int(fileid), int(state), int(sumlev), 
                int(geocomp), chariter, cifsn))
    
        return int(geoid)


        def install(self):
   
            self.log("Install bundle")  
            dest = self.library.put(self)
            self.log("Installed to {} ".format(dest))


class UsCensusFactBundle(UsCensusBundle):
    
    #####################################
    # Peparation
    #####################################
    
    def prepare(self):
        '''Create the prototype database'''

        if not self.database.exists():
            self.database.create()

        self.create_fact_table_schema()
      
        self.make_range_map()

        self.generate_partitions()
 
        return True

                
    def create_fact_table_schema(self):
        '''Uses the generate_schema_rows() generator to creeate rows for the fact table
        The geo split table is created in '''
        from databundles.orm import Column
        
        log = self.log
        tick = self.ptick
        
        commit = False
        
        if len(self.schema.tables) > 20 and len(self.schema.columns) > 400:
            log("Reusing schema")
            return True
            
        else:
            log("Regenerating schema. This could be slow ... ")
        
        log("Generating main table schemas")

        for row in self.generate_schema_rows():

            if row['type'] == 'table':
                
                tick(".")
                name = row['name']
                row['data'] = {'segment':row['segment'], 'fact': True}
                row['commit'] = commit

                del row['segment']
                del row['name']
                t = self.schema.add_table(name, **row )

                # First 5 fields for every record      
                # FILEID           Text (6),  uSF1, USF2, etc. 
                # STUSAB           Text (2),  state/U.S. abbreviation
                # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                #                             Prob only applies to SF2. 
                # CIFSN            Text (2),  characteristic iteration file sequence number
                #                             The number of the segment file             
                # LOGRECNO         Text (7),  Logical Record Number

                # Add all of the foreign keys for the geo tables to the 
                # fact table record.  
                #for fk in self.geo_keys():
                #    self.schema.add_column(t, fk,
                #                           datatype=Column.DATATYPE_INTEGER, 
                #                           is_foreign_key =True,
                #                            commit = commit)
                
                # Add a foreign key reference to the record_code table. 
                self.schema.add_column(t, "record_code_id",
                                           datatype=Column.DATATYPE_INTEGER, 
                                           is_foreign_key =True,
                                           commit = commit)
                
            else:

                if row['decimal'] and int(row['decimal']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
           
                self.schema.add_column(t, row['name'],
                            description=row['description'],
                            datatype=dt,
                            data={'segment':row['segment'],'source_col':row['col_pos']},
                            commit=commit)

        tick("\n")
        

        if not commit: # If we don't commit in the library, must do it here. 
            self.database.session.commit()


    def generate_partitions(self):
        from databundles.partition import PartitionIdentity

        # The Fact partitions
        for table in self.fact_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)


    @property
    def states(self):
        if self._states  is None:
            if 'all' in self.run_args.states:
                states = self.urls['geos'].keys()
                states.sort()  
                self._states = states
            else:
    
                states = [ s for s in self.urls['geos'].keys() if s in self.run_args.states ]
                states.sort()
                self._states = states 
                 
        return self._states

    #############################################
    # Build 
    #############################################
    
    def build(self,  run_state_tables_f=None,run_fact_db_f=None):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        from multiprocessing import Pool

        if self.run_args.subphase in ['test']:
            print self.states
            print self.states_dict
         
 
        # Combine the geodim tables with the  state population tables, and
        # produce .csv files for each of the tables. 
        if self.run_args.subphase in ['all','fact']:   
            if self.run_args.multi and run_state_tables_f:
                
                pool = Pool(processes=int(self.run_args.multi))
          
                result = pool.map_async(run_state_tables_f, enumerate(self.urls['geos'].keys()))
                print result.get()
            else:
                for state in self.states:
                    self.log("Building fact tables for {}".format(state))
                    self.run_state_tables(state)
      
      
        # Load all of the fact table tempfiles into the fact table databases
        # and store the databases in the library. 
        if self.run_args.subphase in ['all','load-fact']:  
            if self.run_args.multi and run_fact_db_f:
                pool = Pool(processes=int(self.run_args.multi))
                
                result = pool.map_async(run_fact_db_f, [ (n,table.id_) for n, table in enumerate(self.fact_tables())])
                print result.get()
            else:
                for table in self.fact_tables():
                    self.run_fact_db(table.id_)
              
        return True
      
    def run_state_tables(self, state):
        '''Split up the segment files into seperate tables, and link in the
        geo splits table for foreign keys to the geo splits. '''
        import time

        fact_partitions = self.fact_partition_map()
       
        with open(self.rangemap_file, 'r') as f:
            range_map = yaml.load(f) 
        
        # Marker to note when the file is done. 
        marker_f = self.filesystem.build_path('markers',state+"_fact_table")
        
        if os.path.exists(marker_f):
            self.log("state table complete for {}, skipping ".format(state))
            return
        else:
            # If it isn't done, remove it if it exists. 
            for partition in fact_partitions.values():
                tf = partition.database.tempfile(partition.table, suffix=state)
                tf.delete()
  
        row_i = 0

        for state, logrecno, geo, segments, geo_keys in self.generate_rows(state, geodim=True ): #@UnusedVariable
 
            if row_i == 0:
                t_start = time.time()
      
            row_i += 1
            
            if row_i % 10000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.log("Fact "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
       
            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems(): #@ReservedAssignment
                    
                    table = self.get_table_by_table_id(table_id)

                    if not segment:
                        #Some segments have fewer lines than others. 
                        #self.error("Failed to get segment data for {}".format(seg_number))
                        continue
                    
                    seg = segment[range['start']:range['end']]
                    
                    if seg and len(seg) > 0:    
                        # The values can be null for the PCT tables, which don't 
                        # exist for some summary levels.       
                        values =  (geo_keys[0],) + geo_keys[3:-1] + tuple(seg) # Remove the state, logrec  and hash from the geo_key  
                        partition = fact_partitions[table_id]
                        tf = partition.database.tempfile(table, suffix=state)

                        if len(values) != len(tf.header):
                            self.error("Fact Table write error. Value not same length as header")
                            print "Segment: ", segment, state, logrecno
                            print "Header : ",len(tf.header), table.name, tf.header
                            print "Values : ",len(values), values
                            print "Range  : ",seg_number, range
                        
                        tf.writer.writerow(values)
                    
                    else:
                        self.log("{} {} Seg {}, table {}  is empty".format(state, logrecno,  seg_number, table_id))

        #
        # Write the values to tempfiles. 
        # 

        for table_id, partition in fact_partitions.items():
            
            table = self.get_table_by_table_id(table_id)
            tf = partition.database.tempfile(table, suffix=state)
                            
            tf.close()

        with open(marker_f, 'w') as f:
            f.write(str(time.time()))

    

    def run_fact_db(self, table_id):
        '''Load the fact table for a single table into a database and
        put it in the library. Copies all of the temp files for the state
        into the database. '''
   
        try:
            table = self.schema.table(table_id)
        except:
            self.error("Could not get table for id: "+table_id)
            return
        
        partition = self.fact_partition(table, False)
        
        if self.library.get(partition) and not self.run_args.test:
            self.log("Found in fact table bundle library, skipping.: "+table.name)
            return
        
        partition = self.fact_partition(table, True)
        
        db = partition.database
        
        try:
            db.clean_table(table) # In case we are restarting this run
        except Exception as e:
            self.error("Failed for "+partition.database.path)
            raise e
            
        for state in self.urls['geos'].keys():
            tf = partition.database.tempfile(table, suffix=state)
        
            print "PATH ",tf.path
            if not tf.exists:
                if self.run_args.test:
                    self.log("Missing tempfile, ignoring b/c in test: {}".format(tf.path))
                    return
                else:
                    raise Exception("Fact table tempfile does not exist table={} state={} path={}"
                                    .format(table.name, state, tf.path) )
            else:
                self.log("Loading fact table for {}, {} from  {} ".format(state, table.name, tf.path))
     
            try:
                db.load_tempfile(tf)
                tf.close()
            except Exception as e:
                self.error("Loading fact table failed: {} ".format(e))
                return 

        dest = self.library.put(partition)
        self.log("Install Fact table in library: "+str(dest))

        partition.database.delete()
        
        for state in self.urls['geos'].keys():
            tf = partition.database.tempfile(table, suffix=state)
            tf.delete()

    #############
    # Fact Table and Partition Acessors. 
    
    def fact_tables(self):
        for table in self.schema.tables:
            if table.data.get('fact',False):
                yield table
            
    def fact_processors(self):
        '''Generate a complete set of processors for all of the fact tables.
        These processors only deal with the forieng keys to the geo split tables. '''
        from databundles.transform import PassthroughTransform
        
        processor_set = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            processors = [PassthroughTransform(c) for c in columns]
     
            processor_set[table.id_] = (table, columns, processors )  
       
        return processor_set   
    
    def fact_partition(self, table, init=False):
        '''Called in geo_partition_map to fetch, and create, the partition for a
        table ''' 
        from databundles.partition import PartitionIdentity
    
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid.name))
        
        if init and not partition.database.exists():
            partition.create_with_tables(table.name)
            
        return partition
    
    def fact_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.fact_tables():
            partitions[table.id_] = self.fact_partition(table)
 
        return partitions 
 
       
def make_geoid(state, county, tract, block=None, blockgroup=None):
    '''Create a geoid for common blocks. This is not appropriate for
    all summary levels, but it is what is used by census.ire.org
    
    See: 
        http://www.census.gov/rdo/pdf/0GEOID_Construction_for_Matching.pdf
        https://github.com/clarinova/census/blob/master/dataprocessing/load_crosswalk_blocks.py
        
    '''
    
    x = ''.join([
            state.rjust(2, '0'),
            county.rjust(3, '0'),
            tract.rjust(6, '0')
            ])
    
    if block is not None:
        x = x + block.rjust(4, '0')
        
    if blockgroup is not None:
        x = x + blockgroup.rjust(4, '0')
    

    

        
    
    