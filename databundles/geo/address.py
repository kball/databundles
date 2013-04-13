'''
Created on Apr 11, 2013

@author: eric
'''

from pyparsing import oneOf, OneOrMore, originalTextFor, Optional,  Combine, FollowedBy, Regex, MatchFirst, Suppress
from pyparsing import Word, CaselessLiteral, White
from pyparsing import alphas, nums, alphanums, Keyword
import pkgutil
import os
import csv
import sys

street_suffixes = {}
suffix_keywords = []
with open(os.path.join(os.path.dirname(sys.modules[__name__].__file__),'support','suffixes.csv'), 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        street_suffixes[row[0]] =  row[1]
        suffix_keywords.append(Keyword(row[0], caseless=True) + Optional(Suppress(".")))

    
#
# From http://pyparsing.wikispaces.com/file/view/streetAddressParser.py
#

# define number as a set of words
units = oneOf("Zero One Two Three Four Five Six Seven Eight Nine Ten"
          "Eleven Twelve Thirteen Fourteen Fifteen Sixteen Seventeen Eighteen Nineteen",caseless=True)
tens = oneOf("Ten Twenty Thirty Forty Fourty Fifty Sixty Seventy Eighty Ninety",caseless=True)
hundred = CaselessLiteral("Hundred")
thousand = CaselessLiteral("Thousand")
OPT_DASH = Optional("-")

numberword = ((( units + OPT_DASH + Optional(thousand) + OPT_DASH + 
                  Optional(units + OPT_DASH + hundred) + OPT_DASH + 
                  Optional(tens)) ^ tens ) 
               + OPT_DASH + Optional(units) )

# number can be any of the forms 123, 21B, 222-A or 23 1/2
housenumber = originalTextFor(
    numberword | 
    Combine(Word(nums) + Optional(OPT_DASH + oneOf(list(alphas))+FollowedBy(White()))) + Optional(OPT_DASH + "1/2")
).setResultsName("number")


blocknumber = Combine(housenumber + "block", joinString=" ", adjacent=False).setResultsName("blocknumber")

numberSuffix = oneOf("st th nd rd").setName("numberSuffix")

streetnumber = originalTextFor( Word(nums) + Optional(OPT_DASH + "1/2") + Optional(numberSuffix) ) 

# street name 
nsew = oneOf("N S E W North South East West NW NE SW SE") + Suppress(Optional("."))


# types of streets - extend as desired
type_suffix = Combine( MatchFirst(suffix_keywords)).setResultsName("street_type").setParseAction(lambda x : street_suffixes[x[0]])

# Wilbur ave
# Wild Burr Ave

simple_named_street = Combine(OneOrMore(~type_suffix + Word(alphas)), joinString=" ", adjacent=False).setResultsName("simple_street")

numbered_street = Combine( Combine(Word(nums) + Optional(OPT_DASH + "1/2") + Optional(numberSuffix)),joinString=" ", adjacent=False).setResultsName("numbered_street") 

direction_street =  oneOf("North South East West") 

streetName =   Combine( 
    Optional(nsew) + numbered_street ^
    Optional(nsew) + simple_named_street ^
    direction_street
, joinString=" ", adjacent=False).setResultsName("street_name")  +  Optional(type_suffix) 

#streetName =  (
#    nsew + streetnumber ^
#    streetnumber ^
#    Combine(~numberSuffix + OneOrMore(~type_suffix + Combine(Word(alphas) + Optional("."))), joinString=" ", adjacent=False) ^
#    Combine(type_suffix + Word(alphas), joinString=" ", adjacent=False)
#    ).setName("streetName").setResultsName("name")
    
    
# PO Box handling
acronym = lambda s : Regex(r"\.?\s*".join(s)+r"\.?")
poBoxRef = (
            (
             acronym("PO") | 
             acronym("APO") | 
             acronym("AFP")
            ) + Optional(CaselessLiteral("BOX"))
           ) + Word(alphanums)("boxnumber")

# basic street address
streetReference = (streetName + Optional(type_suffix))

intersection = ( streetReference +  ( '@' | Keyword("and",caseless=True)) + streetReference )

streetAddress = ( poBoxRef("street") ^ 
                  housenumber + streetReference ^ 
                  blocknumber + streetReference ^ 
                  streetReference ^ 
                  intersection 
                ) + Optional(Suppress(','))

# how to add Apt, Suite, etc.
suiteRef = (
            oneOf("Suite Ste Apt Apartment Room Rm #", caseless=True) + 
            Optional(".") + 
            Word(alphanums+'-')("suitenumber"))

streetAddress = streetAddress + Optional(Suppress(',') + suiteRef("suite"))


city = Word(alphanums+ " ").setResultsName("city") + Optional(Suppress(','))

state = Word(alphanums).setResultsName("state") + Optional(Suppress(','))

zipCode = Regex("\d{5}(?:[-\s]\d{4})?").setResultsName("zipCode")

address = (
          streetAddress + city + state + zipCode |
          streetAddress + city + state |
          streetAddress + city |
          streetAddress
        )

class Address(object):

    def __init__(self):
        '''
        Constructor
        '''
        pass
    
    
    def parse(self, addrstr):

        return  address.parseString(addrstr, parseAll=True)

        
        
        