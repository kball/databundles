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
        
        try:
            _ , self.addresses = library.dep(addressesds)
         
        except ConfigurationError:
            raise
            raise ConfigurationError(("MISSING DEPENDENCY: To get addresses or codes, the configuration  "+
                "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
                "See https://github.com/clarinova/databundles/wiki/Error-Messages#geogeocodergeocoder__init__")
                .format(addressesds))

        
    def geocode_semiblock(self, street, city, state):
        """ Just parses the street,. Expects the city, state and zip to be broken out. """

        try: ps = self.parser.parse(street)
        except: ps = False
        
        if not ps:
            return  []
        
        return self._block_geocode_parts(ps.number, ps.street_name,  ps.street_type, city, state)

    def geocode_address(self, street, city, state):
        """ Just parses the street,. Expects the city, state and zip to be broken out. """

        try: ps = self.parser.parse(street)
        except: ps = False
        
        if not ps:
            return  []
        
        return self._address_geocode_parts(ps.number, ps.street_name,  ps.street_type, city, state)

    def _block_geocode_parts(self, number, street, street_type, city, state):

        if not number:
            return [];

        city = city.title()
        street = street.title()

        queries = [
            ("""SELECT 10 as quality, * FROM geocode_block WHERE  (lcity = ?  or rcity = ? )
            AND street = ? AND street_type = ? AND ? BETWEEN lnumber AND hnumber
            ORDER BY segment_source_id""",(city,  city, street, street_type, number)),
            ("""SELECT 9 as quality, * FROM geocode_block WHERE (lcity = ?  or rcity = ? ) AND 
            street = ? AND ? BETWEEN lnumber AND hnumber
            ORDER BY segment_source_id""",(city, city,  street, number)),
            ("""SELECT 8 as quality, * FROM geocode_block WHERE street = ? AND ? BETWEEN lnumber AND hnumber
            ORDER BY segment_source_id""",(street, number)),
            ("""SELECT 7 as quality, * FROM geocode_block WHERE  (lcity = ?  or rcity = ? )
            AND street = ? AND street_type = ? AND ? BETWEEN lnumber-100 AND hnumber+100
            ORDER BY segment_source_id""",(city,  city, street, street_type, number))
        ]

        for query, args in queries:

            candidates = {}
          
            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                city = ar.get('lcity', ar.get('rcity'))

                r = {
                    'quality': ar['quality'],
                    'addresses_id': None,
                    'segment_source_id':  ar['segment_source_id'],
                    'address_source_id': None,
                    'zip': ar.get('lzip'),
                    'street': ar['street'],
                    'street_dir':  ar['street_dir'],
                    'street_type': ar['street_type'],
                    'x': ar['xm'],
                    'y': ar['ym'],
                    'number': number,
                    'hnumber': ar['hnumber'],
                    'lnumber': ar['lnumber'],
                    'city' : city
                }
                
                candidates.setdefault((city,ar['street'],ar['street_type']),[]).append(r)

            if len(candidates) > 0:
                return candidates

        return []
    
    def _address_geocode_parts(self, number, street, street_type, city, state):

        if not number:
            return [];

        city = city.title()
        street = street.title()

        block_number = int(float(number)/100.0)*100

        queries = [
            (20, """SELECT * FROM addresses WHERE city = ? AND street = ? AND street_type = ? AND number = ?
            ORDER BY segment_source_id""",(city,  street, street_type, number )), 
            (19, """SELECT * FROM addresses WHERE city = ? AND street = ? AND number = ?
            ORDER BY segment_source_id""",(city,  street, number )), 
            (18, """SELECT * FROM addresses WHERE street = ? AND number = ?
            ORDER BY segment_source_id""",(street, number )),
            (17, """SELECT * FROM addresses WHERE city = ? AND street = ? AND street_type = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(city,  street, street_type, block_number,  str(int(block_number)+99)) ), 
            (16, """SELECT * FROM addresses WHERE city = ? AND street = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(city,  street, block_number,  str(int(block_number)+99)) ), 
            (15, """SELECT * FROM addresses WHERE street = ? AND number BETWEEN ? AND ?
            ORDER BY segment_source_id""",(street, block_number,  str(int(block_number)+99)) )
        ]

        return self._do_search(queries, number, street, street_type, city, state)
       
    def _do_search(self, queries, number, street, street_type, city, state):

        if not number:
            return [];

        for quality, query, args in queries:

            candidates = {}
            print query, args
            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                city = ar.get('city', ar.get('rcity'))
                
               
                
                r = {
                    'quality': quality,
                    'addresses_id': ar.get('addresses_id'),
                    'segment_source_id':  ar.get('segment_source_id'),
                    'address_source_id': ar.get('addr_source_id'),
                    'zip': ar.get('zip'),
                    'street': ar['street'],
                    'street_dir': ar.get('street_dir',None),
                    'street_type': ar['street_type'],
                    'x': ar.get('x'),
                    'y': ar.get('y'),
                    'lat': ar.get('lat'),
                    'lon': ar.get('lon'),
                    'number': ar.get('number', ar.get('lnumber')),
                    'city' : city
                }
                
                candidates.setdefault((city,ar['street'],ar['street_type']),[]).append(r)

            if len(candidates) > 0:
                return candidates

        return []

    def get_street_addresses(self, segment_source_id):
        
        addresses = {}
        
        for ar in self.addresses.query("SELECT * FROM addresses WHERE segment_source_id = ?", segment_source_id):
            addresses[ar['number']] = dict(ar)
            
        return addresses
        
        

    
    