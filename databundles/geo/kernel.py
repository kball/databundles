"""Kernels are arrays used in 2D colvolution on analysis area arrays. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from databundles.identity import PartitionIdentity
from osgeo import gdal, gdal_array, osr
from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16
from numpy  import *
  
class Kernel(object):

    def round(self):
        '''Make the matrix spot sort of round, by masking values in the corners'''
        
        # Get the max value for the edge of the enclosed circle. 
        # This assumes that there is a radial gradient. 
        row_max = self.matrix[self.center][0]

        for (x_m,y_m), value in ndenumerate(self.matrix):
            if self.matrix[y_m][x_m] > row_max:
                self.matrix[y_m][x_m] = 0
        
    def norm(self):
        #self.matrix /= sum(self.matrix)
        self.matrix /= self.matrix.max()
        
        
    def invert(self):
        ''''Invert the values, so the cells closer to the center 
        have the higher values. '''
        #range = self.matrix.max() - self.matrix.min() 

        self.matrix = self.matrix.max() - self.matrix
        self.inverted = ~self.inverted
       
    def quantize(self, bins=255):
        from util import jenks_breaks
        hist, edges = histogram(self.matrix.compressed(),bins=bins)
      
        print "Hist", hist
        print "Edges",edges
        
        breaks = jenks_breaks(self.matrix.compressed().tolist(), bins)
        print "Breaks",breaks
        
        l = list(set(self.matrix.compressed().tolist()))
        l.sort()
        print "Uniques", l
        
        print self.matrix.compressed()
        digits = digitize(self.matrix.ravel(), breaks)
        
        print self.matrix.size
        print digits.size
        print self.matrix.shape[0]
        
        s = ma.array(reshape(digits, self.matrix.shape), mask=self.matrix.mask)
        
        print s
      
    def apply(self,a, point,  f=None, v=None):
        """Apply the values in the kernel onto an array, centered at a point. 
        
        :param a: The array to apply to 
        :type a: numpy.array
        :param f: A two argument function that decides which value to apply to the array
        :type f: callable
        :param point: The point, in the array coordinate system, where the center of the
        kernel will be applied
        :type point: Point
        :param v: External value to be passed into the function
        :type v: any
        """
        import numpy as np
        
        if v:
            from functools import partial
            f = partial(f,v)
        
        y_max, x_max = a.shape
        m = None
        use_m=False
        
        
        if point.x < self.offset:
            if point.x < 0:
                return
            
            x_start = max(point.x - self.offset,0)
            x_end = point.x + self.offset +1  
            m = self.matrix[:,(self.offset-point.x):self.matrix.shape[1]]
            use_m=True
            
        elif point.x+self.offset+1 >  x_max :
            if point.x > x_max:
                return
            
            x_start = point.x - self.offset
            x_end = min(point.x + self.offset+1, x_max)
            m = self.matrix[:,0:self.matrix.shape[1]+ (x_max-point.x-self.offset)-1]
            use_m=True
        else:
            x_start = point.x - self.offset
            x_end = point.x + self.offset+1
        
        sm = (m if use_m else self.matrix)
        
        if point.y < self.offset:
            if point.y < 0:
                return
            
            y_start = max(point.y - self.offset,0)
            y_end = point.y + self.offset+1
            m = sm[(self.offset-point.y):sm.shape[0],:]
            use_m=True
        elif point.y+self.offset+1 >  y_max:
            if point.y > y_max:
                return
            
            y_start = point.y - self.offset
            y_end = point.y + self.offset+1
            m = sm[0:sm.shape[0]+ (y_max-point.y-self.offset)-1,:]
            use_m=True
        else:
            y_start = point.y - self.offset
            y_end = point.y + self.offset+1      

        #print a.shape, point, x_start, x_end, y_start, y_end, (m if use_m else self.matrix).shape
        a[y_start:y_end, x_start:x_end] = f( a[y_start:y_end, x_start:x_end], 
                                             (m if use_m else self.matrix) )
                        
        
    def apply_add(self,a,point,y=None):
        from ..geo import Point
        if y is not None:
            point = Point(point, y)
        return self.apply(a,point, f=lambda x,y: x+y)
    
    def apply_min(self,a,point):
        return self.apply(a,point, min)        
    
    def apply_max(self,a,point):
        return self.apply(a,point, max)        
        
class ConstantKernel(Kernel):
    """A Kernel for a constant value"""
    
    def __init__(self, size=1, value =1 ):
        self.value  = value
        self.matrix = ones((size, size))*value
        self.offset = (self.matrix.shape[0] - 1) / 2 
        
        if size > 1:
            self.center =  int(size/2) 
        else:
            self.center = 1
                  
         
class GaussianKernel(Kernel):
    
    def __init__(self, size=9, fwhm=3 ):
        
        
        # For 1-based array indexing, we'd have to +1, but this is zero-based
        self.center =  int(size/2) 
         
        m = self.makeGaussian(size, fwhm)

        m /= sum(m) # Normalize the sum of all cells in the matrix to 1
        

        self.offset = (m.shape[0] - 1) / 2 
        self.matrix = m

    @staticmethod
    def makeGaussian(size, fwhm = 3):
        """ Make a square gaussian kernel.
    
        size is the length of a side of the square
        fwhm is full-width-half-maximum, which
        can be thought of as an effective radius.
        """
    
        x = arange(0, size, 1, float32)
        y = x[:,newaxis]
        x0 = y0 = size // 2
        m =  ma.masked_array(exp(-4*log(2) * ((x-x0)**2 + (y-y0)**2) / fwhm**2))
        
        for (y,x), value in ndenumerate(m):
            if value < 1./1000:
                m[y, x] = 0
        
        return m
           
class DistanceKernel(Kernel):
    ''' Each cell is the distance, in cell widths, from the center '''
    def __init__(self, size): 
        import math
        if size%2 == 0:
            raise ValueError("Aray size must be odd")
        
        self.inverted = False
        
        self.matrix = ma.masked_array(zeros((size,size)), mask=True)
        
        # For 1-based array indexing, we'd have to +1, but this is zero-based
        self.center = center = int(size/2) 
        
        row_max = size - center - 1 # Max value on a horix or vert edge
     
        for (x_m,y_m), value in ndenumerate(self.matrix):
                r  = sqrt( (y_m-center)**2 + (x_m-center)**2)
                if r <= row_max:
                    self.matrix[y_m,x_m] = r

                 