'''
Created on Jan 17, 2013

@author: eric
'''

import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase
import os

     
class Test(TestBase):
 
    def setUp(self):
        self.copy_or_build_bundle()

        self.bundle = Bundle()    
    
    def tearDown(self):
        pass

    def test_basic(self):
        from pprint import pprint
        from databundles.geo.geocoder import Geocoder               
        g = Geocoder(self.bundle.library)               
                     
        max = 10
                          
        with open(os.path.join(os.path.dirname(__file__),'test_geocoder_addresses.txt')) as f:
            for line in f:
                max -= 1
                if not max:
                    break;
                


    def write_error_row(self, code, arg, p, w, address, city):
        
        try: ps = p.parse(address)
        except:  ps = False
        

        if not ps:
            row = [code, arg,  address, city]
        else:
            row = [code, arg, address, city, ps.number, ps.street_direction, ps.street_name, ps.street_type]
            
        w.writerow(row)
            

    def x_test_crime(self):
        from databundles.geo.address import Parser
        from databundles.geo.geocoder import Geocoder
        import csv
                      
        g = Geocoder(self.bundle.library, addresses_ds='geoaddresses')      
        _,incidents = self.bundle.library.dep('crime')
    
        log_rate = self.bundle.init_log_rate(1000)
    
        p = Parser()

        with open(self.bundle.filesystem.path('errors.csv'), 'wb') as f:
            writer = csv.writer(f)
            
            writer.writerow(['code','arg','block_address','city','number','dir','street','type'])
            
            multi_cities = 0.0
            multi_addr = 0.0
            no_response = 0.0
            for i, inct in enumerate(incidents.query("SELECT * FROM incidents limit 100000")):
                row = dict(inct)
    
                candidates = g.geocode_semiblock(row['blockaddress'], row['city'], 'CA')
    
                if  len(candidates) == 0:
                    no_response += 1
                    self.write_error_row('norsp',0, p,writer,row['blockaddress'], row['city'])
                    continue
                elif  len(candidates) != 1:
                    multi_cities += 1
                    self.write_error_row('mcities',len(candidates), p,writer,row['blockaddress'], row['city'])
                    continue
                  
                s =  candidates.popitem()[1]
     
                if len(s) > 3:
                    self.write_error_row('maddr',len(s), p,writer,row['blockaddress'], row['city'])
                    multi_addr +=1
                
                if i > 0:
                    log_rate("{}  cities={}, {}% addr={}, {}%  nrp={}, {}%".format(i, 
                                                                        multi_cities, int(multi_cities/i * 100), 
                                                                        multi_addr, int(multi_addr/i * 100),
                                                                        no_response, int(no_response/i * 100) ))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()