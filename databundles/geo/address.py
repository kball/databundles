'''
Created on Apr 11, 2013

@author: eric
'''

from pyparsing import oneOf, OneOrMore, originalTextFor, Optional,  Combine, FollowedBy, Regex, MatchFirst, Suppress, ZeroOrMore, Upcase, replaceWith
from pyparsing import Word, CaselessLiteral, Literal, CaselessKeyword,  White
from pyparsing import alphas, nums, alphanums, Keyword
from pyparsing import ParseException

import pkgutil
import os
import csv
import sys
from functools import partial
import tokenize, token

def init_rdp():
    cmb = partial(Combine, joinString=" ", adjacent=False )
    
    
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
    housenumber = (
        originalTextFor(
                        numberword | 
                        Combine(Word(nums) + Optional(OPT_DASH + oneOf(list(alphas))+FollowedBy(White()))) + Optional(OPT_DASH + "1/2")
        ).setResultsName("number").setName('house_number') +
        Suppress(Optional(Word(nums))) # SANDAG crime addresses have this odd extra 0          
    )
    
    
    blocknumber = (housenumber + Suppress("block")).setResultsName("blocknumber")
    
    numberSuffix = (
                Suppress(Optional(" ")) + 
                (CaselessLiteral("st") ^ CaselessLiteral("th") ^ CaselessLiteral("nd") ^ CaselessLiteral("rd"))
                +FollowedBy(White())
                ).setName("numberSuffix")
    
    streetnumber = originalTextFor( Word(nums) + Optional(OPT_DASH + "1/2") + Optional(numberSuffix) ) 
    
    
    # types of streets 
    type_suffix = (
        Combine( MatchFirst(suffix_keywords)) + ~FollowedBy(Word(alphas))
    ).setParseAction(lambda x : street_suffixes[x[0]].lower())
    
    
    nsew = (
            CaselessKeyword('North') ^ CaselessKeyword('South') ^  CaselessKeyword('East') ^ CaselessKeyword('West') ^
            CaselessKeyword('NE') ^ CaselessKeyword('NW') ^  CaselessKeyword('SE') ^ CaselessKeyword('SW')  ^
            CaselessKeyword('N') ^ CaselessKeyword('S') ^  CaselessKeyword('E') ^ CaselessKeyword('W')
            ) + Suppress(Optional("."))+ ~FollowedBy(type_suffix)
    
    # Wilbur ave
    # Wild Burr Ave
    
    simple_named_street = cmb(Optional(nsew) + ~nsew + OneOrMore(~type_suffix+Word(alphas+'-')))
    
    numbered_street = (
        cmb( Optional(nsew) + Combine(Word(nums) + Optional(OPT_DASH + "1/2") + Optional(numberSuffix)))
    )
    
    direction_street =  oneOf("North South East West").setName('direction_street')
    
    short_numbered_highway = cmb(
        (CaselessLiteral('SR').setParseAction(replaceWith('Highway')) | 
         CaselessLiteral('I').setParseAction(replaceWith('Interstate')) )+
        Suppress(Optional(White())+Optional(Literal("-"))+Optional(White()))+
        Word(nums) 
    ) + Suppress(Optional(
        CaselessKeyword('nb') | # No idea what this is. From "I - 5 Nb" on Camp Pendelton
        CaselessKeyword('business')
    ))
    
    highway_words = (Word('Highway') | Word('hwy'))
    
    trailing_number_highway = cmb(               
        ZeroOrMore( ~highway_words + Word(alphas) ) +   
        highway_words +
        Word(nums)                 
    )
    
    def set_street_type(s, loc, toks):
        toks['street_type'] = 'highway'
    
    streetName =   (                
        (
         short_numbered_highway |
         trailing_number_highway 
        )("street_name").setParseAction(set_street_type)  
        |
        cmb(
         numbered_street |
         simple_named_street 
        )("street_name") +  Optional(type_suffix("street_type")) 
    )
    
    
    # PO Box handling
    acronym = lambda s : Regex(r"\.?\s*".join(s)+r"\.?")
    poBoxRef = (
                (
                 acronym("PO") | 
                 acronym("APO") | 
                 acronym("AFP")
                ) + Optional(CaselessLiteral("BOX"))
               ) + Word(alphanums)("boxnumber")
    
    
    intersection = ( streetName +  ( '@' | Keyword("and",caseless=True)) + streetName )
    
    streetAddress = ( poBoxRef("street") ^ 
                      blocknumber + streetName ^ 
                      housenumber + streetName ^ 
                      streetName ^ 
                      intersection 
                    ) + Optional(Suppress(','))
    
    streetAddress = housenumber + streetName
    
    # how to add Apt, Suite, etc.
    suiteRef = (
                oneOf("Suite Ste Apt Apartment Room Rm #", caseless=True) + 
                Optional(".") + 
                Word(alphanums+'-')("suitenumber"))
    
    #streetAddress = streetAddress + Optional(Suppress(',')) + Optional(suiteRef("suite"))
    
    
    city = Word(alphanums+ " "+"-").setResultsName("city") + Optional(Suppress(','))
    
    state = Word(alphanums).setResultsName("state") + Optional(Suppress(','))
    
    zipCode = Regex("\d{5}(?:[-\s]\d{4})?").setResultsName("zipCode")
    
    address = (
              streetAddress ^
              streetAddress + city ^
              streetAddress + city + state ^
              streetAddress + city + state + zipCode 
    
            ) + Optional(Optional(Suppress(',')) + "USA") # Added by google geocoder100


    return address, streetName

street_types = None
highway_words = None,
highway_regex = None,
def init_street_types():
    import re
    
    global street_types, highway_words, highway_regex
    
    if not street_types:
        
        street_types = {}
        
        with open(os.path.join(os.path.dirname(sys.modules[__name__].__file__),'support','suffixes.csv'), 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                street_types[row[0].lower()] =  row[1].lower()
            
        highway_words = [ k for k,v in street_types.items() if v == 'hwy']
        highway_regex = r'\b(?:' + '|'.join(highway_words) + r')\b'
                
            
        
            
    return street_types, highway_words, re.compile(highway_regex)

class ParseError(Exception):
    pass

class Parser(object):

    def __init__(self):
        '''
        Constructor
        '''
        pass
    
        self.rdp  = init_rdp()

    def parse(self, addrstr):
     
        if not addrstr.strip():
            return False
           
        bas =  addrstr.split('/')
     
        if len(bas) == 0:
            return False
        elif len(bas) == 1:
            return ParserState(addrstr).parse()
        else:
            ps1 = ParserState(bas[0]).parse()
            if bas[1]:
                ps2 = ParserState(bas[1]).parse()
                ps1.cross_street = ps2
                
        
            return ps1

    def rdp_parse(self, addrstr):
        """Parse an address with the recursive descent parser implemented in pyparsing"""

        if not addrstr.strip():
            raise ParseError("Empty string")

        p = self.rdp.parseString(addrstr, parseAll=True)

    def geocode_validate(self, addrstr):
        """ Validate the  address with google geocoder, then parse.
        This is for the really difficult addresses that don't parse otherwise."""
        from geopy.geocoders.googlev3 import GQueryError
        from geopy import geocoders
        gc = geocoders.GoogleV3()

        try:
            print "Geocoding in address.Parser.parse() "
            r = gc.geocode(addrstr,exactly_one=False)
        except GQueryError as e: 
            
            raise ParseError("Geocoding failed for {} ".format(addrstr)+str(e))
               
        if isinstance(r, list):
            r = r.pop(0)
            
        p = self.rdp.parseString(r[0], parseAll=True)     
               
        return p
  
    
class Scanner():
  
    END = 0
    WORD = 1
    NUMBER = 2
    OTHER = 99
  
    types = {
      END : 'END',
      WORD: 'WORD',
      NUMBER: 'NUMBER',
      OTHER: 'OTHER'
    }
  
    @staticmethod 
    def s_word(scanner, token): 
        return (Scanner.WORD, token.lower().strip('.'))
    
    @staticmethod 
    def s_number(scanner, token): 
        return (Scanner.NUMBER,token)
    
    @staticmethod
    def s_other(scanner, token): 
        return (Scanner.OTHER, token.lower().strip())

    def __init__(self):
        import re
        
        self.scanner = re.Scanner([
            (r"\s+", None),
            (r"[a-zA-Z\.\-]+", self.s_word),
            (r"\d+", self.s_number),
            (r".+", self.s_other),
        ])

    def scan(self, s):
        return self.scanner.scan(s)


class ParserState(object):
    
    
        def __init__(self, s):
            '''
            Constructor
            '''
            import StringIO
        
            self.input = s
            
            self.tokens = []
            s = Scanner()
            self.tokens, rest = s.scan(self.input)
       
            self.tokens.append( (Scanner.END, ''))

            self._saved_tokens = []

            self.ttype = None
            self.toks = None
            self.start = None
            self.end = None
            self.line = None

            self.street_types,self.highway_words,self.highway_regex  = init_street_types()

            self.number = None
            self.is_block = False
            self.street_direction = None
            self.street_name = None
            self.street_type = None
            self.cross_street = None

        def __str__(self):

            a = " ".join([ str(i).title() for i in [self.number, self.street_direction, self.street_name, self.street_type ] if i ])
            
            if self.cross_street:
                return a + " / "+str(self.cross_street)
            else:
                return a
            
        @property
        def dir_street(self):
            """Return all components of the street name as a string, excluding the number and type. Include number
            and direction, if it is set"""
            return " ".join([ str(i).title() for i in [self.street_direction, self.street_name] if i ]).strip()

        @property
        def street(self):
            """Return all components of the street name as a string, excluding the number"""
            return " ".join([ str(i).title() for i in [self.street_direction, self.street_name, self.street_type ] if i ]).strip()

        def fail(self, m=None, expected=None):
            
            message = ("Failed for '{toks}' in '{line}' , type={type_name} "
                       .format(toks=self.toks, type_name= Scanner.types[self.ttype], line=self.input)
                      )
            if expected:
                if isinstance(expected, int):
                    expected = Scanner.types[expected]
                if isinstance(expected, (list, tuple)):
                    expected = ",".join(lambda x: Scanner.types[x], expected)
                    
                message += ". expected: '{}' ".format(expected)
            
            if m:
                message += ". message: {}".format(m)
            
            raise ParseError(message)
        
        LAST = -2
        
        
        def as_dict(self):
            
            return {
                    'number': self.number,
                    'is_block': self.is_block,
                    'street_name':self.street_name,
                    'street_direction':self.street_direction,
                    'street_type':self.street_type
                    }
        
        def next(self, location = 0):
            try:
                self.ttype, self.toks = self.tokens.pop(location)
                return int(self.ttype), self.toks
            except StopIteration:
                return Scanner.END, None

        def unshift(self,type, token):
            """Put a toekn back on the front of the token list. """
            self.tokens = [(type, token)] + self.tokens
            self.ttype, self.toks = (type, token)

        def pop(self):
            """Pop a token from the end, just before the end marker"""
            return self.next(location=-2)
        
        def peek(self, location=0):
            """Look at and item without removing it. Use LAST to peek at the end"""
            try:
                ttype, toks = self.tokens[location]
                return int(ttype), toks
            except StopIteration:
                return Scanner.END, None       

        def has(self, p):
            import re
            """Return true if the remainder of the string has the given token. 
            p may be a string or a rexex. """

            if isinstance(p, basestring):
                return str(p) in [ str(toks) for _, toks in self.tokens  ]
            else:
                return len([ str(toks) for _, toks in self.tokens if re.match(p,str(toks)) ]) > 0

        def find(self,p):
            """Return the position in the remining tokens of the first token that matches the
            string or regex"""
            import re
            
            if isinstance(p, basestring):
                def eq(x):
                    return x == p
            else:
                def eq(x):
                    return re.match(p,str(x))
            
            for i,t in enumerate(self.tokens):
                if eq(t[1]):
                    return i
                
            return False
              
        def pluck(self,p):
            """Remove and return from the remaining tokens the first token that matches the string or regex"""
            
            p = self.find(p)
            
            if p is False:
                return False
            
            return self.next(p)
        
        def save(self):
            """Save the current set of remaining tokens, to restore later"""
            self._saved_tokens.append(list(self.tokens))
            
        def restore(self):
            
            if self._saved_tokens:
                self.tokens = self._saved_tokens.pop(0)
                

        def rest(self):
            """Generator for the remainder of the tokens"""
            
            while True:
                ttype, toks = self.next()
                if ttype == Scanner.END: return
                yield ttype, toks
 
        def remainder(self):
            """ Return the remaining tokens as a string"""
            return " ".join([ str(toks) for _, toks in self.tokens  ])
            
        
        def parse(self):
    
            #
            # See if we have a street type as the last item
            #
            ttype, last_toks = self.peek(self.LAST)
           
            if last_toks.lower() in self.street_types:
                self.street_type = self.street_types[last_toks.lower()]
                self.pop()
                              

            #
            # Start with the number
            #
 
            if self.peek()[0] == Scanner.NUMBER: 
                self.number = int(self.next()[1])
  
            #
            # Remove "block" if it exists. In the SANDAG crime dataset, 
            # There are many entries with "BLOCK" twice. 
            #
            while True:
                t = self.pluck('block')
                if t:
                    self.is_block = True
                    t = self.pluck('of')
                else: 
                    break
                

            self.parse_direction() # N, S, E, W
 
            if self.parse_highway():
                pass
            elif self.parse_numbered_street():
                pass
            elif self.parse_simple_street():
                pass
            else:
                self.fail("Couldn't parse the street name")
  
            return self

        def parse_highway(self):
            import re

            if not self.has(self.highway_regex):
                return False

            self.save()
            
            adj = []
            suffix = []
            hwy_word = None
            number = None
            for ttype, toks in self.rest():
                if not re.match(self.highway_regex, toks):
   
                    if ttype == Scanner.NUMBER:
                        number = toks
                    elif toks in ('sb','nb','eb','wb'):
                        self.street_direction = toks
                    elif toks in ['business','loop']:
                        suffix.append(toks)
                    elif ttype == Scanner.WORD and toks != '-':
                        adj.append(toks)
                else:
                    hwy_word = toks

            if number and hwy_word:
                self.street_type = 'highway'
    
                if re.match(r'^(?:i|interstate)$',hwy_word.strip()):
                    hwy_word = "interstate"
                else:
                    hwy_word = "highway"
    
                self.street_name = " ".join(adj+[hwy_word,str(number)]+suffix).title()
                return True
            else:
                self.restore()
                return False
                
            

        def parse_direction(self):
            
            # If the next token is the end, then this is the name of the street
            if self.peek(1)[0] == Scanner.END:
                return
            
            ttype, toks = self.peek()
            if toks[0] in  ('n','s','e','w'):
                if (len(toks) == 1 or toks in ('north','south','east','west','ne','se','nw','sw')):
                    if len(toks) == 2:
                        self.street_direction = toks.upper()
                    else:
                        self.street_direction = toks[0].upper()
                    
                    self.next() 
                    
                    return True

            return False

        def parse_numbered_street(self):
            '''Parse a street that is names with a number'''
            ttype, toks = self.next()
            if ttype != Scanner.NUMBER:
                self.unshift(ttype, toks)
                return
       
            number = toks
            ordinal = ''
            
            ttype, toks = self.next()
            if toks in ('st','th','nd','rd'):
                ordinal = toks
            else:
                self.unshift(ttype, toks)
                
            self.street_name = str(int(number))+ordinal
       
            return True
        
        def parse_simple_street(self):

            self.street_name = " ".join([ toks.capitalize() for _, toks in self.rest()  ]).title()
            
            return True
        
 
 
                

        