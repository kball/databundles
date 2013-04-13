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
        pass




    def tearDown(self):
        pass


    def test_streets(self):
        from databundles.geo.address import simple_named_street, numbered_street, streetName
        
        d = [
             'wilbur',
             'wil but'
             'wilbur street',
             'wil bur street',
             'wilbur st',
             'wil bur st',   
             'wilbur st.',
             'wil bur st.',             
             '10th street',
             '3rd street',
             '5th st',   
             '10th st.',
             '14th st.',  
             'W 10th street, phoenix, AZ',    
             "N 38 1/2 st"       
             ]

        for a in d:
            p = streetName.parseString(a) #@UndefinedVariable
            print p.dump()
            self.assertEquals('ST',p.get('street_type','ST'))


    def x_test_basic(self):
        
        from databundles.geo.address import Address, type_suffix, streetReference, streetAddress, streetnumber
       
        
        ap = Address()
  
        
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