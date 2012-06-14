'''
Created on Jun 14, 2012

@author: eric
'''
import unittest


class RevealAccess(object):
    """A data descriptor that sets and returns values
       normally and prints a message logging their access.
    """

    def __init__(self, initval=None, name='var'):
        self.val = initval
        self.name = name

    def __get__(self, obj, objtype):
        print 'Retrieving', self.name
        return self.val

    def __set__(self, obj, val):
        print 'Updating' , self.name
        self.val = val

class Test(unittest.TestCase):


    def testName(self):
        
        from databundles.properties import SimpleProperty
        
        class Class2(object):
            x = SimpleProperty('bingo', 'Documentation')
         
        o = Class2()
        print "a "+str(o.x)
        o.x = 'foo';
        print "b "+o.x


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()