""" Definition of a geographic area for which a Raster or Aray will be created. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import ogr #@UnresolvedImport
from numpy import * 
from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16 #@UnresolvedImport
from databundles.geo import Point
from util import create_poly
from databundles.dbexceptions import ConfigurationError

#ogr.UseExceptions()


def get_analysis_area(library, **kwargs):
    """Return an analysis area by name or GEOID
    
    Requires a build dependency for 'extents' such as build.dependencies.extents
    
    Keyword Arguments:
    
        geoid: The geoid of the analysis area
        extents_ds: The name of the dependency for the extents dataset. Defaults to 'extents'
     
    :rtype: An `AnalysisArea` object.
    
    """
    geoid = kwargs.get('geoid')

    extentsds = kwargs.get('extents_ds', 'extents')

    try:
        bundle,_ = library.dep(extentsds)
    except ConfigurationError:
        raise
        raise ConfigurationError(("MISSING DEPENDENCY: To get extents, the configuration  "+
            "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
            "See https://github.com/clarinova/databundles/wiki/Error-Messages#geoanalysisareasget_analysis_area ")
            .format(extentsds))

    if not bundle:
        raise ConfigurationError("Didn't find the dataset declared for the '{}' dependency".format(extentsds))
    
    db = bundle.database
    
    places_t = db.table('places')
    spcs_t = db.table('spcs')
    
    s = db.session
    
    query = (s.query(places_t, spcs_t)
             .join(spcs_t, spcs_t.columns.spcs_id == places_t.columns.spcs_id)
             .filter(places_t.columns.geoid == geoid)
            )
            
    row =  query.first()
    
    if not row:
        raise Exception("Failed to get analysis area record for geoid: {}".format(geoid))

    return AnalysisArea( row[6],row.geoid , # 'name' is used twice, pick the first. 
                      row.eastmin, 
                      row.eastmax, 
                      row.northmin, 
                      row.northmax, 
                      row.lonmin, 
                      row.lonmax, 
                      row.latmin, 
                      row.latmax,
                      row.srid,                       
                      row.srswkt)


def draw_edges(a):
        for i in range(0,a.shape[0]): # Iterate over Y
            a[i,0] = 1
            a[i,1] = 2
            a[i,2] = 3
        
            a[i,a.shape[1]-2] = 2
            a[i,a.shape[1]-1] = 1
                                    
        for i in range(0,a.shape[1]): # Iterate over x
            a[0,i] = 1
            a[1,i] = 2
            a[2,i] = 3
            
            a[a.shape[0]-2,i] = 2
            a[a.shape[0]-1,i] = 1
                  


class AnalysisArea(object):
    
    SCALE = 20
    MAJOR_GRID = 100 # All domensions must be even moduloo this. 
    
    def __init__(self, name, geoid,
                 eastmin, eastmax, northmin, northmax, 
                 lonmin, lonmax, latmin, latmax, 
                 srid, srswkt, scale=SCALE, **kwargs):
        """ 
        
        Args:
        
            scale: The size of a side of a cell in array, in meters. 
        
        """
        self.name = name
        self.geoid = geoid
        self.eastmin = eastmin
        self.eastmax = eastmax
        self.northmin = northmin
        self.northmax = northmax
 
        self.lonmin = lonmin
        self.lonmax = lonmax
        self.latmin = latmin
        self.latmax = latmax
        
        self.srid = srid
        self.srswkt = srswkt
        self.scale = scale # UTM meters per grid area
        
        #Dimensions ust be even by MAJOR_GRID
     
        if  (self.eastmin % self.MAJOR_GRID + self.eastmax % self.MAJOR_GRID +
             self.northmin % self.MAJOR_GRID + self.northmax % self.MAJOR_GRID ) > 0:
            raise Exception("Bounding box dimensions must be even modulo {}"
                            .format(self.MAJOR_GRID))
                                 
        if  self.MAJOR_GRID % self.scale != 0:
            raise Exception("The scale {} must divide evenly into the MAJOR_GRID {}"
                            .format(self.scale, self.MAJOR_GRID))                                                
    
        self.size_x = (self.eastmax - self.eastmin) / self.scale
        self.size_y = (self.northmax - self.northmin) / self.scale

    def new_array(self, dtype=float):
        return zeros((self.size_y, self.size_x), dtype = dtype)



    def new_masked_array(self, dtype=float, nodata=0):
        
        return ma.masked_array(self.new_array(dtype=dtype),nodata)  
        
    @property 
    def state(self):
        '''Extract the state from the geoid.'''
        import re 
        r = re.match('CG(\d\d).*', self.geoid)
        if r:
            return int(r.group(1))
        else:
            raise NotImplementedError("state can only handle AAs with census geoids. ")

    @property
    def lower_left(self):
        return (self.eastmin, self.northmin)
    
    @property
    def upper_left(self):
        return (self.eastmin, self.northmax)
    
    @property
    def pixel_size(self):
        return self.SCALE

    @property
    def srs(self):

        return self._get_srs(self.srid)
      
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
        
    def get_coord_transform(self,  source_srs=None):
        """Get the OGR object for converting coordinates

        """
        s_srs = self._get_srs(source_srs)
        d_srs = self.srs
    
        return ogr.osr.CoordinateTransformation(s_srs, d_srs)
                
    def get_translator(self, source_srs=None):
        """Get a function that transforms coordinates from a source srs
        to the coordinates of this array """
        import math
        
        trans = self.get_coord_transform(source_srs)

        def _transformer(x,y):
            xp,yp,z =  trans.TransformPoint(x,y)
            return Point(
                         int(round((xp-self.eastmin)/self.scale)),
                         int(round((yp-self.northmin)/self.scale))
                    )
        
        return _transformer
  
        
        
    def is_in_ll(self, lon, lat):
        """Return true if the (lat, lon) is inside the area"""
        return (lat < self.latmax and
                lat > self.latmin and
                lon < self.lonmax and
                lon > self.lonmin )     
        
        
    def is_in_ll_query(self, lat_name='lat',lon_name='lon'):
        """Return SQL text for querying for a lat/long point that is in this analysis area"""
        
        return (""" {lat_name} >= {latmin} AND {lat_name} <= {latmax} AND
        {lon_name} >= {lonmin} AND {lon_name} <= {lonmax}"""
        .format( lat_name=lat_name, lon_name=lon_name,**self.__dict__))
        

    @property
    def place_bb_poly(self):
        """Polygon for the bounding box of the place"""
        geo =  create_poly(((self.lonmin,self.latmin),
                                  (self.lonmin,self.latmax),
                                  (self.lonmax,self.latmax),
                                  (self.lonmax,self.latmin)
                                ),self._get_srs(None)
                                 )
        
        return geo
    
    @property
    def area_bb_poly(self):
        """Polygon for the bounding box of the analysis area"""
        geo =  create_poly(((self.eastmin,self.northmin),
                          (self.eastmin,self.northmax),
                          (self.eastmax,self.northmax),
                          (self.eastmax,self.northmin)
                        ), self.srs)
    
    
        return geo
    
    def write_poly(self, file_, layer='poly', poly=None):
        """Write both bounding boxes into a KML file
        
        Write the bounding box area: 
        >>>> aa.write_poly('/tmp/place',layer='place', poly=aa.place_bb_poly)
        >>>> aa.write_poly('/tmp/area',layer='area', poly=aa.area_bb_poly)
        
        """
        import ogr #@UnresolvedImport

        if not file_.endswith('.kml'):
            file_ = file_+'.shp'
   
        #driver = ogr.GetDriverByName('ESRI Shapefile')
        driver = ogr.GetDriverByName('KML')
        
        if poly is None:
            poly = self.area_bb_poly
        
        datasource = driver.CreateDataSource(file_)
        layer = datasource.CreateLayer(layer,
                                       srs = poly.GetSpatialReference(),
                                       geom_type=ogr.wkbPolygon)

     
        #create feature object with point geometry type from layer object:
        feature = ogr.Feature( layer.GetLayerDefn() )
        feature.SetGeometry(poly)      
        layer.CreateFeature(feature)
   
        poly = self.place_bb_poly
     
        #create feature object with point geometry type from layer object:
        feature = ogr.Feature( layer.GetLayerDefn() )
        feature.SetGeometry(poly)      
        layer.CreateFeature(feature)

        #flush memory
        feature.Destroy()
        datasource.Destroy()

    def get_geotiff(self, file_,  bands=1, over_sample=1, data_type=GDT_Int16, nodata=0):
        from osgeo import gdal
    
        driver = gdal.GetDriverByName('GTiff') 
            
        out = driver.Create(file_, 
                            self.size_x*over_sample, self.size_y*over_sample,
                            bands, 
                            data_type, 
                            options = [ 'COMPRESS=LZW' ])  
        
        # Note that Y pixel height is negative to account for increasing
        # Y going down the image, rather than up. 
        transform = [self.eastmin, # self.lower_left[0] ,  # Upper Left X postion
                     self.pixel_size/over_sample ,  # Pixel Width 
                     0 ,     # rotation, 0 if image is "north up" 
                     self.northmax, #self.lower_left[1] ,  # Upper Left Y Position
                     0 ,     # rotation, 0 if image is "north up"
                     -self.pixel_size/over_sample # Pixel Height
                     ]
    
    
        out.SetGeoTransform(transform)  
        for i in range(bands):
            out.GetRasterBand(i+1).SetNoDataValue(nodata)
            
        out.SetProjection( self.srs.ExportToWkt() )
        
        return out

    
    
    def write_geotiff(self, file_,  a, data_type=GDT_Float32, nodata=0):
        """
        Args:
            file_: Name of file to write to
            aa: Analysis Area object
            a: numpy array
        """
        

        out = self.get_geotiff( file_,  data_type=data_type)
     
        out.GetRasterBand(1).SetNoDataValue(nodata)
        out.GetRasterBand(1).WriteArray(flipud(a))
      
        return file_

    @staticmethod
    def rd(v):
        """Round down, to the nearest even 100"""
        import math
        return math.floor(v/100.0) * 100
    
    @staticmethod
    def ru(v):
        """Round up, to the nearest even 100"""
        import math
        return math.ceil(v/100.0) * 100
        

    def get_aa_from_envelope(self, envelope, name=None, geoid=None):
        """Create a new AA given the envelope from a GDAL geometry."""
        import util
        d_srs =  ogr.osr.SpatialReference()
        d_srs.ImportFromEPSG(4326) # Lat/Long in WGS84
    
        trans = ogr.osr.CoordinateTransformation(self.srs, d_srs)

        env1_bb = util.create_bb(envelope, self.srs)
        env1_bb.TransformTo(d_srs)       
        env2_bb = util.create_bb(env1_bb.GetEnvelope(), env1_bb.GetSpatialReference()).GetEnvelope()

        d = {
             'lonmin': env2_bb[0],
             'lonmax': env2_bb[1],
             'latmin': env2_bb[2],
             'latmax': env2_bb[3],
            
             'eastmin': self.rd(envelope[0]),
             'eastmax': self.ru(envelope[1]),
             'northmin': self.rd(envelope[2]),
             'northmax': self.ru(envelope[3])
             }      


        return AnalysisArea( 
                  name,
                  geoid , # 'name' is used twice, pick the first. 
                  srid=self.srid,                       
                  srswkt=self.srswkt,
                  **d)


    def __str__(self):
        return ("AnalysisArea    : {name} \n"+
                "WGS84  Extents  : ({lonmin},{latmin}) ({lonmax},{latmax})\n"+
                "SPZone Extents  : ({eastmin},{northmin}) ({eastmax},{northmax})\n"+
                "Size            : ({size_x}, {size_y})\n"
                "EPGS SRID:     : {srid}\n"+
                "Pro4txt: {proj4txt}"
        ).format(proj4txt=self.srs.ExportToProj4(),**self.__dict__)
        
    