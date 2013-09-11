"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import  PartitionBase, PartitionIdentity
from sqlite import SqlitePartition
from ..database.geo import GeoDb


class GeoPartitionIdentity(PartitionIdentity):
    PATH_EXTENSION = '.db'
    pass
  


class GeoPartition(SqlitePartition):
    '''A Partition that hosts a Spatialite for geographic data'''
    
    FORMAT = 'geo'
    
    def __init__(self, bundle, record):
        super(GeoPartition, self).__init__(bundle, record)

    @property
    def database(self):
        if self._database is None:
            self._database = GeoDb(self.bundle, self, base_path=self.path)          
        return self._database

        
    def get_srs_wkt(self):
        
        #
        # !! Assumes only one layer!
        
        try:
            q ="select srs_wkt from geometry_columns, spatial_ref_sys where spatial_ref_sys.srid == geometry_columns.srid;"
            return self.database.query(q).first()[0]
        except:
            q ="select srtext from geometry_columns, spatial_ref_sys where spatial_ref_sys.srid == geometry_columns.srid;"
            return self.database.query(q).first()[0]

    def get_srs(self):
        import ogr 
        
        srs = ogr.osr.SpatialReference()
        srs.ImportFromWkt(self.get_srs_wkt())
        return srs

    @property
    def srs(self):
        return self.get_srs()

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
        import subprocess
        import csv 
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
 
        rdr = csv.reader(stdout.decode('ascii').splitlines())# local csv module shadows root #@UndefinedVariable
        header = rdr.next()
       
        if not progress_f:
            progress_f = lambda x: x
       
        with arg.database.inserter(table_name) as ins:
            for i, line in enumerate(rdr):
                ins.insert(line)
                progress_f(i)

    def load_shapefile(self, pid=None,  **kwargs):
        """Load a shape file into a partition as a spatialite database. 
        
        Will also create a schema entry for the table speficified in the 
        table parameter of the  pid, using the fields from the table in the
        shapefile
        """
        import subprocess
        from databundles.dbexceptions import ConfigurationError
        from databundles.geo.util import get_shapefile_geometry_types
        import os
        
        pid, name = self._pid_or_args_to_pid(self.bundle, pid, kwargs)
        pid['format'] = 'geo'
        partition = self.new_partition(pid)
        try: extant = self.partitions.find(pid)
        except: extant = None # Fails with ValueError because table does not exist. 
        
        if extant:
            raise Exception('Geo partition already exists for pid: {}'.format(pid.name))                
        
        shape_file=kwargs.get('shape_file')
        
        t_srs=kwargs.get('t_srs')
        
        if t_srs:
            t_srs_opt = '-t_srs EPSG:{}'.format(t_srs)
        else:
            t_srs_opt = ''
        
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

    def __repr__(self):
        return "<geo partition: {}>".format(self.name)
