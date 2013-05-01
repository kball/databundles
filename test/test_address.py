'''
Created on Jan 17, 2013

@author: eric
'''

import unittest

tests = """\
    100 main bypass
    100 del mar vista 
    100 W 10th street
    100 main street, phoenix
    100 main street, phoenix, AZ
    100 W main street, phoenix, AZ
    100 W 10th street, phoenix, AZ
    3120 De la Cruz Boulevard
    100 South Street
    123 Main
    221B Baker Street
    10 Downing St
    1600 Pennsylvania Ave
    33 1/2 W 42nd St.
    454 N 38 1/2
    21A Deer Run Drive
    256K Memory Lane
    12-1/2 Lincoln
    23N W Loop South
    23 N W Loop South
    25 Main St
    2500 14th St
    12 Bennet Pkwy
    Pearl St
    Bennet Rd and Main St
    19th St
    1500 Deer Creek Lane
    186 Avenue A
    2081 N Webb Rd
    2081 N. Webb Rd
    1515 West 22nd Street
    2029 Stierlin Court
    P.O. Box 33170
    The Landmark @ One Market, Suite 200
    One Market, Suite 200
    One Market
    One Union Square
    AVE. B AND GATEVIEW, SAN FRANCISCO, CA
    """.split("\n")

count = '123456789_' *8

class TestBase(unittest.TestCase):
 
    def setUp(self):
        from collections import OrderedDict
        
        self.streets = OrderedDict([
            ('block of wilbur', (None, 'Wilbur',None)),            
            ('wilbur', (None, 'Wilbur',None)),
            ('wil bur', (None, 'Wil Bur',None)),
            ('Carmel Mountain Rd', (None, 'Carmel Mountain','rd')),
            ('Federal Blvd Suite B', (None, 'Federal','blvd')),
            ('Fairmount Ave Suite 300B', (None, 'Fairmount','ave')),
            ('Black Mountain Rd Suite 10', (None, 'Black Mountain','rd')),
            (' Clairemont Mesa Blvd Suite 203', (None, 'Clairemont Mesa','blvd')),
            (' La Jolla Blvd Suite D', (None, 'La Jolla','blvd')),
            (' Silverton Ave Suite 108', (None, 'Silverton','ave')),                     
            ('Esquire Glen', (None, 'Esquire','gln')),
            ('  BLOCK I AVENUE', (None, 'I','ave')),
            ('  BLOCK BLOCK BLOCK ACACIA AVENUE', (None, 'Acacia','ave')),
            ('wilbur st.', (None, 'Wilbur','st')),
            ('wil bur place', (None, 'Wil Bur','pl')),
            ('wilbur pl.', (None, 'Wilbur','pl')),
            ('wil bur st', (None, 'Wil Bur','st')),
            ('E wilbur pl.', ('E', 'Wilbur','pl')),
            ('West wil bur st', ('W', 'Wil Bur','st')),
            ('Shir - Mar Place', (None, 'Shir - Mar','pl')),
            ('E Street', (None, 'E','st')),
            ('E 24 Th Street ', (None, '24th','st')),
            ('Emerald street', (None, 'Emerald','st')),
            ('8 Th Avenue', (None, '8th','ave')),          
            ('Grand Avenue', (None, 'Grand','ave')),
            ('Old Highway 80', (None, 'Old Highway 80','highway')),
            ('Highway 80', (None, 'Highway 80','highway')),
            ('Sr - 67', (None, 'Highway 67','highway')),
            ('I - 8', (None, 'Interstate 8','highway')),
            ('I - 8 Business', (None, 'Interstate 8 Business','highway')),
            ('Via Blanca', (None, 'Via Blanca',None)),
            ('Camto De La Cruz', (None, 'Camto De La Cruz',None)),
            ('West wilbur', (None, 'Wilbur',None)),
            ('wilbur street', (None, 'wilbur','st')),
            ('wil bur street', (None, 'Wil Bur','st')),
            ('E wilbur', ('E', 'Wilbur',None)),
            ('W wil bur', ('W', 'Wil Bur',None)),
            ('SE wilbur street', ('SE', 'Wilbur','st')),
            ('East wilbur', ('E', 'Wilbur',None)),
            ('West wil bur', ('W', 'Wil Bur',None)),
            ('wilbur street', (None, 'Wilbur','st')),
            ('10th street', (None, '10th','st')),
            ('3rd street', (None, '3rd','st')),
            ('5th st', (None, '5th','st')),
            ('10th st.', (None, '10th','st')),
            ('14th st.', (None, '14th','st')),
            ('E 5th st', ('E', '5th','st')),
            ('W 10th st.', ('W', '10th','st')),
            ('SE 14th st.', ('SE', '14th','st')),
            ])


        self.addresses = OrderedDict([
             ('400 F Street , CHULA VISTA, CA 91910',(400,'F','st','CHULA VISTA')),
             ('1900 Camto De La Cruz, CHULA VISTA, CA 91913',(1900,'Camto De La Cruz',None,'CHULA VISTA')),
             ('13400 I - 8 Business, LAKESIDE, CA 92040',(13400,'Interstate 8','highway','LAKESIDE')),
             ('1900 Grand Avenue, CHULA VISTA, CA 91913',(1900,'Grand','ave','CHULA VISTA')),  
             ('1900 E Emerald , CHULA-VISTA, CA 91913',(1900,'E Emerald',None,'CHULA-VISTA')),    
             ('1900 Emerald , CHULA-VISTA, CA 91913',(1900,'Emerald',None,'CHULA-VISTA')),       
             ('1900 0 Emerald , CHULA-VISTA, CA 91913',(1900,'Emerald',None,'CHULA-VISTA')),   
             ('1900 0 8 th st , CHULA-VISTA, CA 91913',(1900,'8th','st','CHULA-VISTA')),       
             ('6700 I - 5 Nb , CHULA VISTA, CA 91913',(6700,'Interstate 5','highway','CHULA-VISTA')),      
             ('6700 I - 5 Nb , CHULA VISTA, CA 91913',(6700,'Interstate 5','highway','CHULA-VISTA')),  
             ('10700 Jamacha , SPRING VALLEY, CA 91978',(10700,'Jamacha',None,'SPRING VALLEY')),  
             ('10700 Jamacha Boulevard , SPRING VALLEY, CA 91977',(10700,'Jamacha','blvd','SPRING VALLEYA')),  
             ('10700 Jamacha Boulevard , SPRING VALLEY, CA 91978',(10700,'Jamacha','blvd','SPRING VALLEY')),  
             ('10700 Jamacha Boulevard , COUNTY UNINCORPORATED, CA 91978',(10700,'Jamacha','blvd','COUNTY UNINCORPORATED')),  
             ('10700 Jamacha , SAN DIEGO, CA',(10700,'Jamacha',None,'SAN DIEGO')),  
             ('10700 block Jamacha , SAN DIEGO, CA',(10700,'Jamacha',None,'SAN DIEGO')),    
        ])


    def tearDown(self):
        pass


    def test_altparser(self):
        from databundles.geo.address import ParserState, Scanner, init_street_types
     
        for street, check in self.streets.items():
        
            ps = ParserState("100 "+street)
         
            ps.parse()
            
            #print street, ps.as_dict()

            if check[0]: self.assertEquals(check[0],ps.street_direction) 
            self.assertEquals(check[1],ps.street_name)
            self.assertEquals(check[2],ps.street_type)
            
        
        
    def test_errors(self):
        from databundles.geo.address import ParserState
        import imp

        bundle = imp.load_source('bundle', 
            '/Users/eric/proj/Bundles/src/civicdata/sandiego.gov/sandiego.gov-businesses-orig/bundle.py')
        b = bundle.Bundle()

        p = b.partitions.find(table='businesses', grain='errors')

        for row in p.query("SELECT * FROM businesses"):
            ps = ParserState(row['address'])
         
            ps.parse()
            
            print ps
            

    def x_test_streets(self):
        from databundles.geo.address import  init_rdp
        _, streetName = init_rdp()

        for a,out in self.streets.items():
            try:
                p = streetName.parseString(a) #@UndefinedVariable
            except:
                print a
                print '123456789_'*8
                raise
            
            try:
                self.assertEquals(out[0],str(p['street_name']))
                self.assertEquals(out[1],p.get('street_type'))
            except:
                print p.dump()
                raise


    def x_test_addresses(self):
        from databundles.geo.address import Parser
        from collections import OrderedDict

        ap = Parser()

        for a,out in self.addresses.items():
            try:
                p = ap.parse(a) #@UndefinedVariable
            except:
                print a
                print '123456789_'*8
                raise
        
            try:
                self.assertEquals(out[0],int(p['number']))
                self.assertEquals(out[1],str(p['street_name']))
                self.assertEquals(out[2],p.get('street_type'))
            except:
                print p.dump()
                raise


    def x_test_basic(self):
        
        from databundles.geo.address import Parser
       
        
        ap = Parser()
  
        
        p = ap.parse('100 10th street')
        print p.dump()
        self.assertTrue(p.get('city',None) is None)
        self.assertEquals('10th', p['street_name'])

        p = ap.parse('100 10th street, city, state')
        print p.dump()
        self.assertEquals('10th', p['street_name'])

        p = ap.parse('100 10th st, city, state')
        print p.dump()

        self.assertEquals('10th', p['street_name'])    

        #p = ap.parse('206 W 10TH st, city, state')
        #print p.dump()

        self.assertEquals('10th', p['street_name'])   

        for t in map(str.strip,tests):
            if t:
                
                print t
                print count
                
                p = ap.parse(t)
                
                print p.dump()
                

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()