'''
Create an OGR shapefile from a schema
'''
import ogr, osr
import os
import os.path
from databundles.orm import Column
from databundles.dbexceptions import ConfigurationError

ogr_type_map = { 
        None: ogr.OFTString,
        Column.DATATYPE_TEXT: ogr.OFTString,
        Column.DATATYPE_VARCHAR: ogr.OFTString,
        Column.DATATYPE_INTEGER: ogr.OFTInteger,
        Column.DATATYPE_INTEGER64: ogr.OFTInteger,
        Column.DATATYPE_NUMERIC: ogr.OFTReal,       
        Column.DATATYPE_REAL: ogr.OFTReal,       
        Column.DATATYPE_FLOAT: ogr.OFTReal,       
        Column.DATATYPE_DATE: ogr.OFTDate, 
        Column.DATATYPE_TIME: ogr.OFTTime, 
        Column.DATATYPE_TIMESTAMP: ogr.OFTDateTime, 
        Column.DATATYPE_DATETIME: ogr.OFTDateTime, 
        }

class TableShapefile(object):

    def __init__(self, bundle, path, table, dest_srs=4326, source_srs=None, name = None):

        self.bundle = bundle
        self.path = path
        self.table = self.bundle.schema.table(table)

        if not self.table:
            raise ConfigurationError("Didn't find table: {}".format(table))

        basename, extension = os.path.splitext(self.path)

        if extension[1:] == 'zip':
            self.compress = True
            basename, extension = os.path.splitext(basename)
            self.path = basename+extension
        else:
            self.compress = False

        if not extension:
            self.format = 'shapefile'
        else:
            self.format = extension[1:]

        if self.format in ('kml','geojson'):
            dest_srs=4326
      
        self.srs = self._get_srs(dest_srs)
      
        if source_srs:
            self.source_srs = self._get_srs(source_srs)
        else:
            self.source_srs = None

        if self.source_srs:
            self.transform = osr.CoordinateTransformation(self.source_srs, self.srs)
        else:
            self.transform = None

        self.ds = self.create_datasource(self.path, self.format)

        self.type, self.geo_col_names, self.geo_col_pos  = self.figure_feature_type()

        self.layer = None

        if name:
            self.name = str(name)
        else:
            self.name = str(self.table.name)
      
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        
        self.close()
               
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            return False
                
        return self
      
    def figure_feature_type(self):
        
        typ = None
        geo_col_names = [None, None]
        geo_col_pos = [None, None]
        
        
        # First look for a geometry column. If it exists, the x/y, or lon/lat
        # columns are secondary, for the centroid. 
        for i,c in enumerate(self.table.columns):
            if c.name == 'geometry' or c.name == 'wkb' or c.name == 'wkt':
                typ = c.datatype
                geo_col_names[0] = c.name
                geo_col_pos[0] = i
                
                return typ ,  geo_col_names,    geo_col_pos 
     
        vals = 0 
        for i,c in enumerate(self.table.columns):       
            if c.name == 'lat' or c.name == 'y':
                typ = 'point'
                geo_col_names[1] = c.name
                geo_col_pos[1] = i
                vals += 1
            elif  c.name == 'lon' or c.name == 'x':
                typ = 'point'
                geo_col_names[0] = c.name
                geo_col_pos[0] = i
                vals += 1
                
            if vals == 2:
                break

        return typ ,  geo_col_names,    geo_col_pos 
                
        
    def load_schema(self, layer):
        """Create fields definitions in the layer"""
        for c in self.table.columns:
            
            if c.name.lower() in ('wkt','wkb','geometry'):
                continue
            
            dt = c.datatype
            size = c.size
            
            if self.format == 'shapefile':
                if dt == Column.DATATYPE_TIME:
                    dt = Column.DATATYPE_TEXT
                    size = 8
                    
            
            fdfn =  ogr.FieldDefn(str(c.name), ogr_type_map[dt] )
            
            if self.format == 'shapefile':
                if dt == Column.DATATYPE_TEXT:
                    if not size:
                        raise ConfigurationError("Column {} must specify a size for shapefile output".format(c.name))
                    fdfn.SetWidth(size)

            layer.CreateField(fdfn)
          
    def geo_vals(self, row):
        """Return the geometry fields from a row. Returnes a two item tuple, 
        with (x,y) for a point, or (Geometry,non) for blob, wbk or wkt geometry"""

        if self.type == 'point':
            if isinstance(row, dict):
                return (row[self.geo_col_names[0]], row[self.geo_col_names[1]])
            else:
                return (row[self.geo_col_pos[0]], row[self.geo_col_pos[1]])
            
        else:
            if isinstance(row, dict):
                return (row[self.geo_col_names[0]], None)
            else:
                return (row[self.geo_col_pos[0]], None)
        
    def get_geometry(self, row):

        x,y = self.geo_vals(row)
            
        if self.type == 'point':
            geometry = ogr.Geometry(ogr.wkbPoint)
            geometry.SetPoint_2D(0, x, y )
                
        elif self.geo_col_names[0] == 'geometry':
            # ? Not sure what this is?
            geometry = ogr.CreateGeometryFromWkt(x)
        elif self.geo_col_names[0] == 'wkt':
            geometry = ogr.CreateGeometryFromWkt(x)
        elif self.geo_col_names[0] == 'wkb':    
            geometry = ogr.CreateGeometryFromWkb(x)
        else:
            raise Exception("Didn't find geometery column")

        if geometry:
            if not geometry.TransformTo(self.srs):
                raise Exception("Failed to transform Geometry")
        else:
            
            raise Exception("Didn't get a geometry object: x="+str(x)+" type="+str(self.type)+" gcn="+self.geo_col_names[0])
            
        return geometry
            
    def add_feature(self, row, source_srs=None):
        import datetime
        
        geometry = self.get_geometry(row)
        
        if source_srs is not None and source_srs != self.source_srs:
            self.source_srs = self._get_srs(source_srs)
            self.transform = osr.CoordinateTransformation(self.source_srs, self.srs)
            
        if self.layer is None:
            type_ =  geometry.GetGeometryType() 
            self.layer = self.ds.CreateLayer( self.name, self.srs, type_)
            
            if self.layer is None:
                raise Exception("Failed to create layer {} ".format(self.name))
            
            self.load_schema(self.layer)

        
        feature = ogr.Feature(self.layer.GetLayerDefn())

        if isinstance(row, dict):
            for i,c in enumerate(self.table.columns):
                if i not in self.geo_col_pos or c.name in ['x','y','lat','lon']:
                    v = row.get(c.name, False)
                  
                    if v is not False and  isinstance(v, basestring):
                        try:
                            v = str(v.decode('latin1').encode('utf_8') if v else None)
                        except Exception :
                            print row
                            raise
   
                    if v is not False:
                        if isinstance(v, datetime.date):         
                            feature.SetField(str(c.name), v.year, v.month, v.day, 0, 0, 0, 0)
                        elif isinstance(v, datetime.datetime):
                            feature.SetField(str(c.name), v.year, v.month, v.day, v.hour, v.minute, v.second, 0)
                          
                        else:
                            feature.SetField(str(c.name), str(v))
                    elif c.default:
                        try: feature.SetField(str(c.name), c.python_type(c.default))
                        except:
                            print "Failed for {} ".format(c.name)
                            raise
                      
        else:
            for i,v in enumerate(row):
                if i not in self.geo_col_pos:
                    feature.SetField(i, row.get(v, c.python_type(c.default) if c.default else None) )
            
        if self.transform:
            geometry.Transform(self.transform)
            
        feature.SetGeometryDirectly(geometry)
        self.layer.CreateFeature(feature)
        feature.Destroy()
   

    def _get_srs(self, srs_spec=None, default=4326):
        
        # Sometimes the EPSG numbers come from the database as strings
        try: srs_spec = int(srs_spec)
        except: pass
        
        srs = ogr.osr.SpatialReference()
        
        if srs_spec is None and default is not None:
            return self._get_srs(default, None)
            srs.ImportFromEPSG(default) # Lat/Long in WGS84
        elif isinstance(srs_spec,int):
            srs.ImportFromEPSG(srs_spec)
        elif  isinstance(srs_spec,basestring):
            srs.ImportFromWkt(srs_spec)
        elif isinstance(srs_spec, ogr.osr.SpatialReference ):
            return srs_spec
        else:
            raise ValueError("Bad srs somewhere. Source={}, Default = {}"
                             .format(srs_spec, default))
            
        return srs
    
    def create_datasource(self, path, fmt):
        import os
        from databundles.util import rm_rf

        options = []
        
        if fmt == 'kml':
            drv = ogr.GetDriverByName( "KML" )
        elif fmt == 'geojson':
            drv = ogr.GetDriverByName( "GeoJSON" )
        elif fmt == 'sqlite' or fmt == 'db':
            drv = ogr.GetDriverByName( "SQLite" )
            options = ['SPATIALITE=YES', 'INIT_WITH_EPSG=YES','OGR_SQLITE_SYNCHRONOUS=OFF', 
                       'OGR_SQLITE_CACHE=1024', '-gt 50000', 'COMPRESS_GEOM=yes']
        elif fmt == 'shapefile':
            drv = ogr.GetDriverByName( "ESRI Shapefile" )
        else: 
            raise Exception("Unknown format: {} ".format(fmt))
            
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        
        if os.path.exists(path):
            if os.path.isdir(path):
                rm_rf(path)
            else:
                os.remove(path)
        
        ds = drv.CreateDataSource(path, options=options)
         
        
        if ds is None:
            raise Exception("Failed to create datasource: {}".format(path))

        return ds
    
    def compress_file(self):
        import zipfile
        import os

        with zipfile.ZipFile(self.path+'.zip', 'w') as zf:
            if os.path.isdir(self.path):
                for f in os.listdir(self.path):
                    absf = os.path.join(self.path,f)
                    if os.path.isfile(absf):
                        zf.write(absf, self.name+'/'+f)
            else:
                zf.write(self.path)

    def close(self):
        
        self.ds.SyncToDisk()
        self.ds.Release()
        
        
        if self.compress:
            self.compress_file()
        
        

