'''

'''


class Geocoder(object):

    def __init__(self, library, **kwargs):
        """
        
        Args:
            geocoder_ds: addresses dataset dependency name. Defaults to 'addresses'
        """
        
        from databundles.geo.address import Parser
        from databundles.dbexceptions import ConfigurationError
        
        self.parser = Parser()
        
        addressesds = kwargs.get('geocoder_ds', 'geocoder')
        
        
        try:
            _ , self.addresses = library.dep(addressesds)
         
        except ConfigurationError:
            raise ConfigurationError(("MISSING DEPENDENCY: To get addresses or codes, the configuration  "+
                "must specify a dependency with a set named '{0}', in build.dependencies.{0}"+
                "See https://github.com/clarinova/databundles/wiki/Error-Messages#geogeocodergeocoder__init__")
                .format(addressesds))
         
    def geocode(self, street, city):
        """Calls either geocode_street() geocode_intersection()"""
        
        result = None
        if ' / ' in  street:
            
            s1, s2 = street.split('/',1)
            result = self.geocode_intersection(s1,s2, city.strip())
       
        else:
            try:address = self.geocode_street(street, city.strip())
            except Exception as e:
                raise
                address = None

            if address and address['score'] > 20:
                result = address
                
        return result
          
         
    def geocode_street(self, street, city):
        """Geocode an address to a street segment"""
        try: ps = self.parser.parse(street)
        except: ps = False
    
        if not ps:
            return None

        direction = ps.street_direction
        street = ps.street_name
        street_type = ps.street_type
        number = ps.number
        
        q = """SELECT  * FROM segments WHERE street = ?""";

        by_scode, by_name = self.jur_codes()

        # If this fails, the "city" is probably an unincorporated place, which is in the county. 
        try: in_city = by_name[city]
        except: in_city = 'SndSDO'

        max_score = 0
        winner = None
       
        for s in self.addresses.query(q, street):
            
            s= dict(s)
             
            s['score']  = score = self.rank_street(by_name, s, number,  direction, street_type, in_city)

      
            if in_city == s['rcity']:
                s['city'] = s['rcity']   
            elif in_city == s['lcity']:
                s['city'] = s['lcity']   
            else:
                s['city'] = None                 

          
            if not winner or score > max_score:
                winner = s
                max_score = score

        if winner:
            winner['lat'] = winner['latc']
            winner['lon'] = winner['lonc']
            winner['x'] = winner['xc']
            winner['y'] = winner['yc']
            winner['gctype'] = 'cns/segment'
            winner['gcquality'] = winner['score']     
            
        return winner
    
    
    def geocode_intersection(self, street1, street2, city):

        try: 
            ps1 = self.parser.parse(street1)
            ps2 = self.parser.parse(street2)
        except:
            return None
    
        if not ps1 or not ps2:
            return None

        q = """SELECT  * FROM nodes 
        WHERE street_1 = ? and street_2 = ?
        OR street_1 = ? and street_2 = ? LIMIT 1""";
      
        intr = self.addresses.query(q, ps1.street_name, ps2.street_name, ps2.street_name, ps1.street_name).first()

        if intr:
            winner = dict(intr)
            winner['gctype'] = 'cns/intersection'
            return winner
        else:
            return None
        
    def jur_codes(self):
        
        by_scode = {}
        by_name = {}
        for place in self.addresses.query("SELECT code, scode, name FROM places WHERE type = 'city'"):
            by_scode[place['scode']] = (place['code'], place['name'])
            by_name[place['name']] = place['code']
          
        by_name['County Unincorporated'] = 'SndSDO'
        by_name['Unincorporated'] = 'SndSDO'
          
        return by_scode, by_name
       
    def rank_street(self, by_name, row, number, direction, street_type, city ):
        """ Create a score for a street segment based on how well it match es the input"""
        
        score = 0
        
        if (row['street_dir'] or direction) or (not row['street_dir']  and not direction):
            if row['street_dir'] == direction:
                score += 10
            
        if row['street_type'] == street_type:
            score += 10    

        if city == row['rcity'] or city == row['lcity']:
            score += 20  

             

        if number >= row['lnumber'] and number <=row['hnumber']:
            score += 17
        elif number:
            numdist = min( abs(number-row['lnumber']), abs(number-row['hnumber']))
            
            if numdist < 1500:
                score += int((1500-numdist) / 100) # max of 15 points
        
    
        return score

    def _do_search(self, queries, number, street, street_type, city, state):

        if not number:
            return [];

        for quality, query, args in queries:

            candidates = {}
            print query, args
            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                city = city.title() if city else ar.get('city', ar.get('rcity', None))

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

      
    def geocode_semiblock(self, street, city, state):
        """ Just parses the street,. Expects the city, state and zip to be broken out. """

        try: ps = self.parser.parse(street)
        except: ps = False
        
        if not ps:
            return  []

        number = ps.number
        street = ps.street_name
        street_type = ps.street_type

        if not number:
            return [];

        city = city.title()
        street = street.title()

        queries = [
            ("""SELECT 10 as gcquality, * FROM segments WHERE  (lcity = ?  or rcity = ? )
            AND street = ? AND street_type = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(city,  city, street, street_type, number)),
                   
            ("""SELECT 9 as gcquality, * FROM segments WHERE (lcity = ?  or rcity = ? ) AND 
            street = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(city, city,  street, number)),
                   
            ("""SELECT 8 as gcquality, * FROM segments WHERE (lcity = ?  or rcity = ? ) AND 
            street = ? AND ? BETWEEN lnumber AND hnumber
            ORDER BY hnumber ASC""",(city, city,  street, number)),
                   
            ("""SELECT 7 as gcquality, * FROM segments WHERE street = ? AND ? BETWEEN lnumber AND hnumber
            AND has_addresses = 1
            ORDER BY hnumber ASC""",(street, number)),
                   
        ]

        for query, args in queries:

            candidates = {}
          
            for ar in self.addresses.query(query, *args  ):
                ar = dict(ar)
                
                candidates.setdefault(ar['segment_source_id'],[]).append(ar)

            if len(candidates) > 0:
                return candidates

        return {}
    
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


    