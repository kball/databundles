'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir

        
    def tearDown(self):
        pass

    def x_test_flfi(self):
        from databundles.util import FileLikeFromIter, copy_file_or_flo
        from StringIO import StringIO
        
     
        strs = ['12345','67890','1234','56789012','34567890']
        
        def cb(a):
            print a
        
        flo = FileLikeFromIter(iter(strs))
        
        n = 17
        
        d = flo.read(n)
        i = 100
        while d and i:
            print d,
            d = flo.read(n)
            i = i - 1
        
        print
        
        
        #s = StringIO()
        #copy_file_or_flo(flo, s)
        #print s
        
        
    def x_test_basic(self):

        m = memoryview(bytearray('1234567890'))
        
        print m.tobytes()
        
        a = b'aaa'
        p = 4
        m[p:(p+len(a))]=b'aaa'
            
        print m.tobytes()
        
        
    def test_decompress(self):

        import sys
        from databundles.util import StreamingGZip
        
        f = open('/tmp/compressed.gz','rb')

        gf = StreamingGZip(fileobj=f)


        d = gf.read(1000)
        print
        while d:
        
            #sys.stdout.write(d)
            d = gf.read(1000)
            print '-',d
        
            

        
        
        f.close()
        
        
        
        
        
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())