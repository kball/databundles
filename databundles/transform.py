"""Support functions for transforming rows read from input files before
writing to databases. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

def coerce_int(v):   
    '''Convert to an int, or return if isn't an int'''
    try:
        return int(v)
    except:
        return v
    
def coerce_int_except(v, msg):   
    '''Convert to an int, throw an exception if it isn't'''
   
    try:
        return int(v)
    except:
        raise ValueError("Bad value: '{}'; {} ".format(v,msg) )
  
def coerce_float(v):   
    '''Convert to an float, or return if isn't an int'''
    try:
        return float(v)
    except:
        return v
    
def coerce_float_except(v, msg):   
    '''Convert to an float, throw an exception if it isn't'''
    try:
        return float(v)
    except:
        raise ValueError("Bad value: '{}'; {} ".format(v,msg) )
                
class PassthroughTransform(object):
    '''
    Pasthorugh the value unaltered
    '''

    def __init__(self, column, useIndex=False):
        """
        """
                # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column: row[column.sequence_id-1]
        else:
            f = lambda row, column=column: row[column.name]

        self.f = f

        
    def __call__(self, row):
        return self.f(row)
       
                
class BasicTransform(object):
    '''
    A Callable class that will take a row and return a value, cleaned according to
    the classes cleaning rules. 
    '''

    @staticmethod
    def basic_defaults(v, column, default, f):
        '''Basic defaults method, using only the column default and illegal_value
        parameters. WIll also convert blanks and None to the default '''
        if v is None:
            return default
        elif v == '':
            return default
        elif str(v) == column.illegal_value:
            return default
        else:
            return f(v)

    def __init__(self, column, useIndex=False):
        """
        
        """
        self.column = column
  
        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits here 
        if str(column.datatype) == 'integer' or str(column.datatype) == 'integer64' :
            #f = lambda v: int(v)
            msg = column.name
            f = lambda v, msg = msg: coerce_int_except(v, msg)
        elif column.datatype == 'real':
            #f = lambda v: int(v)
            msg = column.name
            f = lambda v, msg = msg: coerce_float_except(v, msg)
        else:
            f = lambda v: v

        if column.default is not None:
            if column.datatype == 'text':
                default = column.default 
            else:
                default = int(column.default)
        else:
            default = None
        
        if default:
            f = (lambda v, column=column, f=f, default=default, defaults_f=self.basic_defaults : 
                    defaults_f(v, column, default, f) )

        # Strip test values, but not numbers
        f = lambda v, f=f:  f(v.strip()) if isinstance(v,basestring) else f(v)
        
        
        if useIndex:
            f = lambda row, column=column, f=f: f(row[column.sequence_id-1])
        else:
            f = lambda row, column=column, f=f: f(row[column.name])
        
        self.f = f

        
    def __call__(self, row):
        return self.f(row)
       
       
class CensusTransform(BasicTransform):
    '''
    Transformation that condsiders the special codes that the Census data may
    have in integer fields. 
    ''' 
    
    @staticmethod
    def census_defaults(v, column, default, f):
        '''Basic defaults method, using only the column default and illegal_value
        parameters. WIll also convert blanks and None to the default '''
        if v is None:
            return default
        elif v == '':
            return default
        elif column.illegal_value and str(v) == str(column.illegal_value):
            return default
        elif isinstance(v, basestring) and v.startswith('!'):
            return -2
        elif isinstance(v, basestring) and v.startswith('#'):
            return -3
        else:
            return f(v)
    
    def __init__(self, column, useIndex=False):
        """
        A Transform that is designed for the US Census, converting codes that
        apear in Integer fields. The geography data dictionary in 
        
            http://www.census.gov/prod/cen2000/doc/sf1.pdf
            
        
        Assignment of codes of nine (9) indicates a balance record or that 
        the entity or attribute does not exist for this record.

        Assignment of pound signs (#) indicates that more than one value exists for 
        this field and, thus, no specific value can be assigned.

        Assignment of exclamation marks (!) indicates that this value has not yet 
        been determined or this file.
        
        This transform makes these conversions: 
        
            The Column's illegal_value becomes -1
            '!' becomes -2
            #* becomes -3

        Args:
            column an orm.Column
            useIndex if True, acess the column value in the row by index, not name
            

        """
        self.column = column
  
        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits here 
        if column.datatype == 'integer'  or str(column.datatype) == 'integer64' :
            msg = column.name
            f = lambda v, msg = msg: coerce_int_except(v, msg)
        elif column.datatype == 'real' or column.datatype == 'float':
            msg = column.name
            f = lambda v, msg = msg: coerce_float_except(v, msg)
        else:
           
            # This answer claims that the files are encoded in IBM850, but for the 2000
            # census, latin1 seems to work correctly. 
            # http://stackoverflow.com/questions/2477360/character-encoding-for-us-census-cartographic-boundary-files
            
            # Unicode, et al, is $#^#% horrible, so we're punting and using XML encoding, 
            # which we will claim is to make the name appear correctly in web pages.       
            f = lambda v: v.strip().decode('latin1').encode('ascii','xmlcharrefreplace')

        if column.default and column.default.strip():
            if column.datatype == 'text':
                default = column.default 
            elif column.datatype == 'real' or column.datatype == 'float':
                default = float(column.default) 
            elif column.datatype == 'integer'  or str(column.datatype) == 'integer64' :
                default = int(column.default) 
            else:
                raise ValueError('Unknown column datatype: '+column.datatype)
        else:
            default = None
        
        
        f = (lambda v, column=column, f=f, default=default, defaults_f=self.census_defaults : 
                    defaults_f(v, column, default, f) )

        # Strip test values, but not numbers
        f = lambda v, f=f:  f(v.strip()) if isinstance(v,basestring) else f(v)

        # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column, f=f: f(row[column.sequence_id-1])
        else:
            f = lambda row, column=column, f=f: f(row[column.name])

        
        self.f = f
    
class RowTypeTransformBuilder(object):
    
    def __init__(self):
        self.types = []
    
    def append(self, name, type_):
        self.types.append((name,type_))
        
    def makeListTransform(self):
        import uuid
        
        f_name = "f"+str(uuid.uuid4()).replace('-','')
        
        o = """
def {}(row):

    stripped_num = lambda x: x.strip() if isinstance(x, basestring) else x
    is_not_nothing = lambda x: True if x!='' and x != None else False
    
    try:
        return [
""".format(f_name)
    
        for i,(name,type_) in enumerate(self.types):
            if i != 0:
                o += ',\n'
                
            if type_ == float or type_ == int:
                o += "{type}(row[{i}]) if is_not_nothing(stripped_num(row[{i}])) else None".format(type=type_.__name__,i=i)
            else:
                o += "{type}(row[{i}].strip()) if is_not_nothing(row[{i}]) else None".format(type=type_.__name__,i=i)
            
            
        names = ','.join([ "('{}',{})".format(name, type_.__name__) for name,type_ in self.types])
            
        o+="""
        ]
    except ValueError as e:
        for i,(name,type_) in  enumerate( [{names}] ):
            try:
                type_(row[i].strip()) if row[i] else None
            except ValueError:
                raise ValueError("Failed to convert value '{{}}' in field '{{}}' to '{{}}'".format(row[i], name,type_.__name__))
        raise
""".format(names=names)
 
        exec(o)
    
        return locals()[f_name]
         
    def makeDictTransform(self):
        import uuid
        
        f_name = "f"+str(uuid.uuid4()).replace('-','')
        
        o = "def {}(row):\n    return {{".format(f_name)
    
        for i,(name,type_) in enumerate(self.types):
            if i != 0:
                o += ',\n'
                
            if type_ == str:
                type_ = unicode
                
            if type_ == float or type_ == int:
                o += "'{name}':{type}(row['{name}']) if row['{name}'] != '' and  row['{name}'] is not None else None".format(type=type_.__name__,name=name)
            else:
                o += "'{name}':{type}(row['{name}'])".format(type=type_.__name__,name=name)
            
        o+= '}\n'
 
        #print o
          
        exec(o)
            
            
        return locals()[f_name]
            
    