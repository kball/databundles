"""Access to common geographic datasets

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import yaml
import sys
from databundles.dbexceptions import ConfigurationError


class US:
    """ Access to US states, regions, etc. """
    def __init__(self, library):
        self.library = library
        try:
            self.bundle, _ = self.library.dep('usgeo')
        except ConfigurationError:
            raise ConfigurationError("MISSING DEPENDENCY: "+"To use the US geo datasets, the bundle ( or library  ) must specify a"+
               " dependency with a set named 'usgeo', in build.dependencies.usgeo")
                    
    
    @property
    def states(self):
        return [ USState(self.library, row) for row in self.bundle.query('SELECT * FROM states')]
       
    def state(self,abbrev=None,**kwargs):
        """Retrieve a state record by abbreviation, fips code, ansi code or census code
        
        The argument to the function is a keyword that can be:
        
            abbrev    Lookup by the state's abbreviation
            fips      Lookup by the state's fips code
            ansi      Lookup by the state's ansi code
            census    Lookup by the state's census code
        
        
        Note that the ansi codes are represented as integers, but they aren't actually numbers; 
        the codes have a leading zero that is only maintained when the codes are used as strings. This
        interface returnes the codes as integers, with the leading zero removed. 
        
        """

        if kwargs.get('abbrev') or abbrev:
            
            if not abbrev:
                abbrev = kwargs.get('abbrev')
            
            rows = self.bundle.query("SELECT * FROM states WHERE stusab = ?", abbrev.upper() )
        elif kwargs.get('fips'):
            rows = self.bundle.query("SELECT * FROM states WHERE state = ?", int(kwargs.get('fips')))
        elif kwargs.get('ansi'):
            rows = self.bundle.query("SELECT * FROM states WHERE statens = ?", int(kwargs.get('ansi')))
        elif kwargs.get('census'):
            rows = self.bundle.query("SELECT * FROM states WHERE statece = ?", int(kwargs.get('ansi')))
        else:
            rows = None
            

        if rows:
            return USState(self.library, rows.first())     
        else:
            return None
        
                
    
class USState:
    """Represents a US State, with acessors for counties, tracks, blocks and other regions
    
    This object is a wrapper on the state table in the geodim dataset, so the fields in the object
    that are acessible through _-getattr__ depend on that table, but are typically: 
    
    geoid     TEXT    
    region    INTEGER    Region
    division  INTEGER    Division
    state     INTEGER    State census code
    stusab    INTEGER    State Abbreviation
    statece   INTEGER    State (FIPS)
    statens   INTEGER    State (ANSI)
    lsadc     TEXT       Legal/Statistical Area Description Code
    name      TEXT    

    Additional acessors include:
    
    fips    FIPS code, equal to the 'state' field
    ansi    ANSI code, euals to the 'statens' field
    census  CENSUS code, equal to the 'statece' field
    usps    Uppercase state abbreviation, equal to the 'stusab' field

    
    """
    
    def __init__(self,library, row):
        self.library = library
        self.row = row
        
    def __getattr__(self, name):
        return self.row[name]
       
    @property
    def fips(self):
        return self.row['state']
    
    @property
    def ansi(self):
        return self.row['statens']
    
    @property
    def census(self):
        return self.row['statece']
    
    @property
    def usps(self):
        return self.row['stusab']
            
    def __str__(self):
        return "<{}:{}>".format('USState',self.row['name']);
        
        
    