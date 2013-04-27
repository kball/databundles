'''

'''


class Geocoder(object):

    def __init__(self, library, **kwargs):
        """
        
        Args:
            address_ds: addresses dataset dependency name. Defaults to 'addresses'
            codes_ds: city jurisdiction codes, like sangis.org-streets-orig-429e
        """
        
        from databundles.geo.address import Parser
        from databundles.dbexceptions import ConfigurationError
        
        self.parser = Parser()
        
        addressesds = kwargs.get('addresses_ds', 'addresses')
        codesds = kwargs.get('codes_ds', 'codes')
        
        try:
            _ , self.addresses = library.dep(addressesds)
            self.codes, _ = library.dep(codesds)
            
        except ConfigurationError:
            raise
            raise ConfigurationError(("MISSING DEPENDENCY: To get addresses or codes, the configuration  "+
                "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
                "See https://github.com/clarinova/databundles/wiki/Error-Messages#geogeocodergeocoder__init__")
                .format(addressesds))

        
        self.zips =  {int(row['key']):row['value'] for row in 
                  self.codes.database.query("SELECT * FROM codes WHERE `group` = 'zips' ")}
        
        self.places =  {row['key']:row['value'] for row in 
                  self.codes.database.query("SELECT * FROM codes WHERE `group` = 'places' ")}
        
        self.jurisdiction =  {row['key']:row['value'] for row in 
                  self.codes.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}
           
        jurisdiction = dict(self.jurisdiction)
        jurisdiction['CN'] = 'S.D. County'
        self.rjurisdiction = { v:k for k, v in jurisdiction.items() }
        
        self.rjurisdiction['County Unincorporated'] = 'CN'
        
    def geocode_semiblock(self, street, city, state):
        """ Just parses the street,. Expects the city, state and zip to be broken out. """

        try: ps = self.parser.parse(street)
        except: ps = False
        
        if not ps:
            return  []
        
        return self.block_geocode_parts(ps.number, ps.street_name,  ps.street_type, city, state)

            
    def block_geocode_parts(self, number, street, street_type, city, state):

        if not number:
            return [];

        try: jcode = self.rjurisdiction[city.title()]
        except KeyError: jcode = "CN"

        queries = [
            (10, """SELECT * FROM addresses WHERE city = ? AND street = ? AND street_type = ? AND number BETWEEN ? AND ?
            ORDER BY street_source_id""",(jcode,  street, street_type, number,  str(int(number)+99)) ), 
            (9, """SELECT * FROM addresses WHERE city = ? AND street = ? AND number BETWEEN ? AND ?
            ORDER BY street_source_id""",(jcode,  street, number,  str(int(number)+99)) ), 
            (8, """SELECT * FROM addresses WHERE street = ? AND number BETWEEN ? AND ?
            ORDER BY street_source_id""",(street, number,  str(int(number)+99)) )
        ]

        for quality, query, args in queries:

            candidates = {}

            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                r = {
                    'quality': quality,
                    'addresses_id': ar['addresses_id'],
                    'street_source_id':  ar['street_source_id'],
                    'address_source_id': ar['addr_source_id'],
                    'zip': ar['zip'],
                    'street': ar['street'],
                    'street_dir': ar.get('street_dir',None),
                    'street_type': ar['street_type'],
                    'x': ar['x'],
                    'y': ar['y'],
                    'lat': ar['lat'],
                    'lon': ar['lon'],
                    'number': ar['number'],
                    'city' : self.jurisdiction[ar['city']]
                }
                
                candidates.setdefault((ar['city'],ar['street'],ar['street_type']),[]).append(r)

            if len(candidates) > 0:
                return candidates

        return []
    
    
    