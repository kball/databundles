"""Access classess and identity for partitions. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os

from databundles.identity import PartitionIdentity
from sqlalchemy.orm.exc import NoResultFound

        
class Partition(object):
    '''Represents a bundle partition, part of the bundle data broken out in 
    time, space, or by table. '''
    
    def __init__(self, bundle, record):
        from databundles.database import PartitionDb
        
        self.bundle = bundle
        self.record = record
        
        self._db_class = PartitionDb
        self._database =  None
        self._hd5file = None
        self._tempfile_cache = {}
     
    def init(self):
        '''Initialize the partition, loading in any SQL, etc. '''
    
    @property
    def name(self):
        return self.identity.name
    
    @property
    def identity(self):
        return self.record.identity
    
    def _path_parts(self):

        name_parts = self.bundle.identity.name_parts(self.bundle.identity)
       
        source =  name_parts.pop(0)
        p = self.identity
        # HACK HACK HACK!
        # The table,space,time,grain order must match up with PartitionIdentity._path_str
        partition_path = [ str(i) for i in [p.table,p.space,p.time,p.grain, p.format] if i is not None]
       
        return source,  name_parts, partition_path 
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''
        source,  name_parts, partition_path = self._path_parts()

        return  os.path.join(self.bundle.database.base_path,  *partition_path )

    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        return  os.path.join(self.path,*args)

    @property
    def database(self):
        if self._database is None:
            
            
            source,  name_parts, partition_path = self._path_parts() #@UnusedVariable

            self._database = self._db_class(self.bundle, self, base_path=self.path)
            
            def add_type(database):
                from databundles.bundle import BundleDbConfig
                config = BundleDbConfig(self.database)
                config.set_value('info','type','partition')
                
            self._database.add_post_create(add_type) 
          
        return self._database

    def query(self,*args, **kwargs):
        """Convience function for self.database.query()"""
     
        return self.database.query(*args, **kwargs)
        

    def tempfile(self, table=None, suffix=None,ignore_first=False):
        '''Return a tempfile object for this partition'''
        
        ckey = (table,suffix)

        tf = self._tempfile_cache.get(ckey, None)   
        if tf:
            return tf
        else:                
            if table is None and self.table:
                table = self.table;
            tf = self.database.tempfile(table, suffix=suffix, ignore_first=ignore_first)
            self._tempfile_cache[ckey] = tf
            return tf
      
    @property
    def hdf5file(self):
        from  databundles.hdf5 import Hdf5File
        if self._hd5file is None:
            self._hd5file = Hdf5File(self)
            
        return self._hd5file

    @property
    def data(self):
        return self.record.data
    
    
    @property
    def table(self):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
        
        table_spec = self.identity.table
        
        if table_spec is None:
            return None
        
        return self.bundle.schema.table(table_spec)
        
    def create_with_tables(self, tables=None, clean=False):
        '''Create, or re-create,  the partition, possibly copying tables
        from the main bundle
        
        Args:
            tables. String or Array of Strings. Specifies the names of tables to 
            copy from the main bundle. 
            
            clean. If True, delete the database first. Defaults to true. 
        
        '''

        if not tables: 
            raise ValueError("'tables' cannot be empty")

        if not isinstance(tables, (list, tuple)):
            tables = [tables]

        if clean:
            self.database.delete()

        self.database.create(copy_tables = False)

        self.add_tables(tables)

    def add_tables(self,tables):

        for t in tables:
            if not t in self.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t) #@UnusedVariable
                t_meta.create_all(bind=self.database.engine)       

    def create(self):

        tables = self.data.get('tables',[])

        if tables:
            self.create_with_tables(tables=tables)
        else:
            self.database.create(copy_tables = False)


    @property
    def extents(self, where=None):
        '''Return the bounding box for the dataset. The partition must specify 
        a table
        
        '''
        import geo.util
        return geo.util.extents(self.database,self.table.name, where=where)
        
    def inserter(self, table_or_name=None,**kwargs):
        
        if not self.database.exists():
            self.create()

        return self.database.inserter(table_or_name,**kwargs)

    def __repr__(self):
        return "<partition: {}>".format(self.name)


class HdfPartition(Partition):
    '''A Partition that hosts a Spatialite for geographic data'''
    
    def __init__(self, bundle, record):
        super(HdfPartition, self).__init__(bundle, record)
        
        from .database import HdfDb

        self._db_class = HdfDb

    @property
    def database(self):
        if self._database is None:
            source,  name_parts, partition_path = self._path_parts() #@UnusedVariable
            self._database = self._db_class(self.bundle, self, base_path=self.path)
            self._database.open()
          
        return self._database


class GeoPartition(Partition):
    '''A Partition that hosts a Spatialite for geographic data'''
    
    def __init__(self, bundle, record):
        super(GeoPartition, self).__init__(bundle, record)
        from .database import GeoDb

        self._db_class = GeoDb

    def get_srs_wkt(self):
        
        #
        # !! Assumes only one layer!
        
        q ="select srs_wkt from geometry_columns, spatial_ref_sys where spatial_ref_sys.srid == geometry_columns.srid;"
        
        return self.database.query(q).first()[0]

    def get_srs(self):
        import ogr 
        
        srs = ogr.osr.SpatialReference()
        srs.ImportFromWkt(self.get_srs_wkt())
        return srs

    def get_transform(self, dest_srs=4326):
        """Get an ogr transform object to convert from the SRS of this partition 
        to another"""
        import ogr, osr
        
      
        srs2 = ogr.osr.SpatialReference()
        srs2.ImportFromEPSG(dest_srs) 
        transform = osr.CoordinateTransformation(self.get_srs(), srs2)

        return transform

    def create(self, dest_srs=4326, source_srs=None):

        from databundles.geo.sfschema import TableShapefile
        
        tsf = TableShapefile(self.bundle, self._db_class.make_path(self), self.identity.table,
                             dest_srs = dest_srs, source_srs = source_srs )
        
        tsf.close()
        
        self.add_tables(self.data.get('tables',None))

    def convert(self, table_name, progress_f=None):
        """Convert a spatialite geopartition to a regular arg
        by extracting the geometry and re-projecting it to WGS84
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
                
        """
        import subprocess, csv
        from databundles.orm import Column
        from databundles.dbexceptions import ConfigurationError

        #
        # Duplicate the geo arg table for the new arg
        # Then make the new arg
        #

        t = self.bundle.schema.add_table(table_name)
        
        ot = self.table
        
        for c in ot.columns:
            self.bundle.schema.add_column(t,c.name,datatype=c.datatype)
                
        
        #
        # Open a connection to spatialite and run the query to 
        # extract CSV. 
        #
        # It would be a lot more efficient to connect to the 
        # Spatialite procss, attach the new database, the copt the 
        # records in SQL. 
        #
        
        try:
            subprocess.check_output('spatialite -version', shell=True)
        except:
            raise ConfigurationError('Did not find spatialite on path. Install spatialite')
        
        # Check the type of geometry:
        p = subprocess.Popen(('spatialite {file} "select GeometryType(geometry) FROM {table} LIMIT 1;"'
                              .format(file=self.database.path,table = self.identity.table)), 
                             stdout = subprocess.PIPE, shell=True)
        
        out, _ = p.communicate()
        out = out.strip()
        
        if out == 'POINT':
            self.bundle.schema.add_column(t,'_db_lon',datatype=Column.DATATYPE_REAL)
            self.bundle.schema.add_column(t,'_db_lat',datatype=Column.DATATYPE_REAL)
            
            command_template = """spatialite -csv -header {file} "select *,   
            X(Transform(geometry, 4326)) AS _db_lon, Y(Transform(geometry, 4326)) AS _db_lat 
            FROM {table}" """  
        else:
            self.bundle.schema.add_column(t,'_wkb',datatype=Column.DATATYPE_TEXT)
            
            command_template = """spatialite -csv -header {file} "select *,   
            AsBinary(Transform(geometry, 4326)) AS _wkb
            FROM {table}" """              

        self.bundle.database.commit()

        pid = self.identity
        pid.table = table_name
        arg = self.bundle.partitions.new_partition(pid)
        arg.create_with_tables()

        #
        # Now extract the data into a new database. 
        #

        command = command_template.format(file=self.database.path,
                                          table = self.identity.table)

        
        self.bundle.log("Running: {}".format(command))
        
        p = subprocess.Popen(command, stdout = subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        
        #
        # Finally we can copy the data. 
        #
        
        reader = csv.reader(stdout.decode('ascii').splitlines())
        header = reader.next()
       
        if not progress_f:
            progress_f = lambda x: x
       
        with arg.database.inserter(table_name) as ins:
            for i, line in enumerate(reader):
                ins.insert(line)
                progress_f(i)


class Partitions(object):
    '''Continer and manager for the set of partitions. 
    
    This object is always accessed from Bundle.partitions""
    '''
    
    def __init__(self, bundle):
        self.bundle = bundle

    def partition(self, arg, db_type=None):
        '''Get a local partition object from either a Partion ORM object, or
        a partition name
        
        Arguments:
        arg    -- a orm.Partition or Partition object. 
        
        '''

        from databundles.orm import Partition as OrmPartition
        from databundles.identity import PartitionNumber, PartitionIdentity
        
        if isinstance(arg,OrmPartition):
            orm_partition = arg
        elif isinstance(arg, str):
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==arg ).one()
        elif isinstance(arg, PartitionNumber):
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==str(arg) ).one()
        elif isinstance(arg, PartitionIdentity):  
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==str(arg.id_) ).one()     
        else:
            raise ValueError("Arg must be a Partition or PartitionNumber")

        if orm_partition.data.get('db_type', False):
            db_type = orm_partition.data.get('db_type')
        elif db_type:
            orm_partition.data['db_type'] = db_type
            s = self.bundle.database.session    
            s.merge(orm_partition)
            s.commit()

        if db_type == 'geo':
            return GeoPartition(self.bundle, orm_partition)
        elif db_type == 'hdf':
            return HdfPartition(self.bundle, orm_partition)
        else:
            return Partition(self.bundle, orm_partition)

    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition).count()
    
    @property 
    def all(self): #@ReservedAssignment
        '''Return an iterator of all partitions'''
        from databundles.orm import Partition as OrmPartition
        import sqlalchemy.exc
        try:
            s = self.bundle.database.session      
            return [self.partition(op) for op in s.query(OrmPartition).all()]
        except sqlalchemy.exc.OperationalError:
            return []
            
        
    def __iter__(self):
        return iter(self.all)

            
    @property
    def query(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        
        return s.query(OrmPartition)
 
    
    def get(self, id_):
        '''Get a partition by the id number 
        
        Arguments:
            id_ -- a partition id value
            
        Returns:
            A partitions.Partition object
            
        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique
        ''' 
        from databundles.orm import Partition as OrmPartition
        
        # This is needed to flush newly created partitions, I think ... 
        self.bundle.database.session.close()
        
        if isinstance(id_, PartitionIdentity):
            id_ = id_.identity.id_
            
        
        q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.id_==str(id_).encode('ascii')))
      
        try:
            orm_partition = q.one()
          
            return self.partition(orm_partition)
        except NoResultFound:
            orm_partition = None
            
        if not orm_partition:
            q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.name==id_.encode('ascii')))
            
            try:
                orm_partition = q.one()
              
                return self.partition(orm_partition)
            except NoResultFound:
                orm_partition = None
            
        return orm_partition

    def find_table(self, table_name):
        '''Return the first partition that has the given table name'''
        
        for partition in self.all:
            if partition.table and partition.table.name == table_name:
                return partition
            
        return None

    def find(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        import sqlalchemy.orm.exc
        try:
            
            partitions = [ self.partition(op) for op in self.find_orm(pid, **kwargs).all()];
            
            if len(partitions) == 1:
                return partitions.pop()
            elif len(partitions) > 1 :
                from databundles.dbexceptions import ResultCountError
                
                rl = "; ".join([p.identity.name for p in partitions])
                
                raise ResultCountError("Got too many results: {}".format(rl)) 
            else:
                return None
            
        except sqlalchemy.orm.exc.NoResultFound: 
            return None
   
    
    def find_all(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        ops = self.find_orm(pid, **kwargs).all()
        
        return [ self.partition(op) for op in ops]

    def _pid_or_args_to_pid(self, bundle,  pid, args):
        from databundles.identity import Identity, new_identity
        

        if isinstance(pid, Identity):
            return pid, None
        elif isinstance(pid,basestring):
            return None, pid # pid is actually the name
        elif args.get('name', False):
            return None, args.get('name', None)
        else:
            return new_identity(args, bundle=bundle), None

    
    def find_orm(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        An ORM object is returned, so changes can be persisted. '''
        import sqlalchemy.orm.exc

        pid, name = self._pid_or_args_to_pid(self.bundle, pid, kwargs)
        
        from databundles.orm import Partition as OrmPartition
        q = self.query
        
        if name is not None:
            q = q.filter(OrmPartition.name==name)
        else:       
            if pid.time is not None:
                q = q.filter(OrmPartition.time==pid.time)
    
            if pid.space is not None:
                q = q.filter(OrmPartition.space==pid.space)
        
            if pid.grain is not None:
                q = q.filter(OrmPartition.grain==pid.grain)
       
            if format is not None:
                q = q.filter(OrmPartition.format==pid.format)
        
            if pid.table is not None:
            
                tr = self.bundle.schema.table(pid.table)
                
                if not tr:
                    raise ValueError("Didn't find table named {} ".format(pid.table))
                
                q = q.filter(OrmPartition.t_id==tr.id_)

        return q
    
   
    def new_orm_partition(self, pid, **kwargs):
        '''Create a new ORM Partrition object, or return one if
        it already exists '''
        from databundles.orm import Partition as OrmPartition, Table
     
        s = self.bundle.database.session
   
        if pid.table:
            q =s.query(Table).filter( (Table.name==pid.table) |  (Table.id_==pid.table) )
            table = q.one()
        else:
            table = None
         
        # 'tables' are additional tables that are part of the partion ,beyond the one in the identity
        # Probably a bad idea. 
        tables = kwargs.get('tables',kwargs.get('table',pid.table if pid else None))
    
        if tables and not isinstance(tables, (list,tuple)):
            tables = [tables]
    
        if tables and pid and pid.table and pid.table not in tables:
            tables = list(tables)
            tables.append(pid.table)
         
        data=kwargs.get('data',{})
        
        data['tables'] = tables
        
        if kwargs.get('db_type'):
            data['db_type'] = kwargs.get('db_type')
        
         
        d = pid.to_dict()
        
        try: del d['table'] # OrmPartition requires t_id instead
        except: pass

         
        op = OrmPartition(
             t_id = table.id_ if table else None,
             d_id = self.bundle.identity.id_,
             data=data,
             state=kwargs.get('state',None),
             **d
             )  

        return op

    def clean(self):
        from databundles.orm import Partition as OrmPartition
       
        s = self.bundle.database.session
        s.query(OrmPartition).delete()
        
    def new_partition(self, pid=None, **kwargs):
 
        pid, _ = self._pid_or_args_to_pid(self.bundle, pid, kwargs)
 
        extant = self.find_orm(pid, **kwargs).all()
        
        for p in extant:
            if p.name == pid.name:
                return self.partition(p, db_type=kwargs.get('db_type',None))
       
        op = self.new_orm_partition(pid, **kwargs)
        s = self.bundle.database.session
        s.add(op)   
        s.commit()     
        
        p = self.partition(op, db_type=kwargs.get('db_type',None))
        return p

    def new_geo_partition(self, pid=None, **kwargs):
        
        if kwargs.get('shape_file'):
            return self._new_geo_partition_from_shape( pid, **kwargs)
        else:
            kwargs['db_type'] = 'geo'
            return self.new_partition(pid, **kwargs)
        
    def new_hdf_partition(self, pid=None, **kwargs):
        
        if pid:
            pid.format = 'hdf'
        
        kwargs['db_type'] = 'hdf'
        return self.new_partition(pid, **kwargs)
        
    def _new_geo_partition_from_shape(self, pid=None,  **kwargs):
        """Load a shape file into a partition as a spatialite database. 
        
        Will also create a schema entry for the table speficified in the 
        table parameter of the  pid, using the fields from the table in the
        shapefile
        """
        import subprocess
        from databundles.dbexceptions import ConfigurationError
        from databundles.geo.util import get_shapefile_geometry_types
        
        shape_file=kwargs.get('shape_file')
        
        t_srs=kwargs.get('t_srs')
        
        if t_srs:
            t_srs_opt = '-t_srs EPSG:{}'.format(t_srs)
        else:
            t_srs_opt = ''
            
        pid, name = self._pid_or_args_to_pid(self.bundle, pid, kwargs)
        
        try: extant = self.partitions.find(pid)
        except: extant = None # Fails with ValueError because table does not exist. 
        
        if extant:
            raise Exception('Geo partition already exists for pid: {}'.format(pid.name))
        
        if shape_file.startswith('http'):
            shape_url = shape_file
            shape_file = self.bundle.filesystem.download_shapefile(shape_url)
        
        try:
            subprocess.check_output('ogr2ogr --help-general', shell=True)
        except:
            raise ConfigurationError('Did not find ogr2ogr on path. Install gdal/ogr')
        
        self.bundle.log("Checking types in file")
        types, type = get_shapefile_geometry_types(shape_file)
        
        #ogr_create="ogr2ogr -explodecollections -skipfailures -f SQLite {output} -nlt  {type} -nln \"{table}\" {input}  -dsco SPATIALITE=yes"
        
        ogr_create="ogr2ogr  -progress -skipfailures -f SQLite {output} -gt 65536 {t_srs} -nlt  {type} -nln \"{table}\" {input}  -dsco SPATIALITE=yes"
        
        if not pid.table:
            raise ValueError("Pid must have a table name")
         
        table_name = pid.table
        
        t = self.bundle.schema.add_table(pid.table)
        self.bundle.database.commit()
        
        partition = self.new_partition(pid, db_type='geo')
        
        dir_ = os.path.dirname(partition.database.path)
        if not os.path.exists(dir_):
            self.bundle.log("Make dir: "+dir_)
            os.makedirs(dir_)
        
        cmd = ogr_create.format(input = shape_file,
                                output = partition.database.path,
                                table = table_name,
                                type = type,
                                t_srs = t_srs_opt
                                 )
        
        self.bundle.log("Running: "+ cmd)
    
        output = subprocess.check_output(cmd, shell=True)

        for row in partition.database.connection.execute("pragma table_info('{}')".format(table_name)):
            self.bundle.schema.add_column(t,row[1],datatype = row[2].lower())

        return partition


    def find_or_new(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''
        
        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition
    
        tables = kwargs.get('tables',kwargs.get('table',pid.table if pid else None))
    
        if tables and not isinstance(tables, (list,tuple)):
            tables = [tables]
    
        if tables and pid and pid.table and pid.table not in tables:
            tables.append(partition.identity.table)

        partition = self.new_partition(pid, **kwargs)
        
        if tables:   
            partition.create_with_tables(tables)  

        return partition;
    
    def find_or_new_geo(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''
        
        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition

        partition = self.new_geo_partition(pid, **kwargs)

        return partition;
    
    def find_or_new_hdf(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''

        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition

        partition = self.new_hdf_partition(pid, **kwargs)

        return partition;
    
    def delete(self, partition):
        from databundles.orm import Partition as OrmPartition

        q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.id_==partition.identity.id_))
      
        q.delete()

