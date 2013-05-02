'''
Create an OGR shapefile from a schema
'''
import ogr, osr
import os
from databundles.orm import Column
from databundles.dbexceptions import ConfigurationError

ogr_type_map = { 
        None: ogr.OFTString,
        Column.DATATYPE_TEXT: ogr.OFTString,
        Column.DATATYPE_INTEGER: ogr.OFTInteger,
        Column.DATATYPE_INTEGER64: ogr.OFTInteger,
        Column.DATATYPE_NUMERIC: ogr.OFTReal,       
        Column.DATATYPE_REAL: ogr.OFTReal,       
        Column.DATATYPE_FLOAT: ogr.OFTReal,       
        Column.DATATYPE_DATE:ogr.OFTDate, 
        Column.DATATYPE_TIME: ogr.OFTTime, 
        Column.DATATYPE_TIMESTAMP: ogr.OFTDateTime, 
        }

class SFSchema(object):

    def __init__(self, bundle, path, table, dest_srs=4326, source_srs=None):

        self.bundle = bundle
        self.path = path
        self.table = self.bundle.schema.table(table)

        if not self.table:
            raise ConfigurationError("Didn't find table: {}".format(table))

        basename, extension = os.path.splitext(path)

        if not extension:
            self.format = 'shapefile'
        else:
            self.format = extension[1:]

        if format in ('kml','geojson'):
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

        self.ds = self.create_datasource(path, self.format)

        self.type, self.geo_col_names, self.geo_col_pos  = self.figure_feature_type()
        
        self.layer = None


    def figure_feature_type(self):
        
        typ = None
        geo_col_names = [None, None]
        geo_col_pos = [None, None]
        for i,c in enumerate(self.table.columns):
            if c.name == 'geometry' or c.name == 'wkb' or c.name == 'wkt':
                typ = c.datatype
                geo_col_names[0] = c.name
                geo_col_pos[0] = i
            elif c.name == 'lat' or c.name == 'y':
                typ = 'point'
                geo_col_names[1] = c.name
                geo_col_pos[1] = i
            elif  c.name == 'lon' or c.name == 'x':
                typ = 'point'
                geo_col_names[0] = c.name
                geo_col_pos[0] = i
                
        return typ ,  geo_col_names,    geo_col_pos 
                
        
    def load_schema(self, layer):
        """Create fidls definitions in the layer"""
        for c in self.table.columns:
            
            if c.name in ('wkt','wkb','geometry'):
                continue
            
            fdfn =  ogr.FieldDefn(str(c.name), ogr_type_map[c.datatype] )
            
            if c.datatype == Column.DATATYPE_TEXT:
                if not c.size:
                    raise ConfigurationError("Column {} must specify a size".format(c.name))
                fdfn.SetWidth(c.size)
                
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
            geometry = ogr.CreateGeometryFromWkb(x)
        elif self.geo_col_names[0] == 'wkt':
            geometry = ogr.CreateGeometryFromWkt(x)
        elif self.geo_col_names[0] == 'wkb':    
            geometry = ogr.CreateGeometryFromWkt(x)

        if geometry:
            if not geometry.TransformTo(self.srs):
                raise Exception("Failed to transform Geometry")
            
        return geometry
            
    def add_feature(self, row):

        geometry = self.get_geometry(row)
        
        if self.layer is None:
            type_ =  geometry.GetGeometryType() 
            self.layer = self.ds.CreateLayer( str(self.table.name), self.srs, type_)
            self.load_schema(self.layer)

        
        feature = ogr.Feature(self.layer.GetLayerDefn())

        if isinstance(row, dict):
            for i,c in enumerate(self.table.columns):
                if i not in self.geo_col_pos:
                    if row.get(c.name, False):
                        feature.SetField(i, str(row[c.name]))
        else:
            for i,v in enumerate(row):
                if i not in self.geo_col_pos:
                    feature.SetField(i, row.get(v, None) )
            
        if self.transform:
            geometry.Transform(self.transform)
            
        feature.SetGeometryDirectly(geometry)
        self.layer.CreateFeature(feature)
        feature.Destroy()
        

    def _get_srs(self, srs_spec=None, default=4326):
        
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


        options = []
        
        if fmt == 'kml':
            drv = ogr.GetDriverByName( "KML" )
        elif fmt == 'geojson':
            drv = ogr.GetDriverByName( "GeoJSON" )
        elif fmt == 'sqlite' or fmt == 'db':
            drv = ogr.GetDriverByName( "SQLite" )
            options = ['SPATIALITE=YES', 'INIT_WITH_EPSG=YES','OGR_SQLITE_SYNCHRONOUS=OFF']
        elif fmt == 'shapefile':
            drv = ogr.GetDriverByName( "ESRI Shapefile" )
        else: 
            raise Exception("Unknown format: {} ".format(fmt))
            
        
        ds = drv.CreateDataSource(path, options=options)
         
        if ds is None:
            raise Exception("Failed to create datasource: {}".format(path))
            
        return ds
    
    def close(self):
        self.ds.Destroy()

