'''
Created on Sep 7, 2013

@author: eric
'''
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class InserterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def insert(self, row, **kwargs): raise NotImplemented()
    
    def close(self): raise NotImplemented()

class UpdaterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def update(self, row): raise NotImplemented()
    
    def close(self): raise NotImplemented()


class SegmentInserterFactory(object):
    
    def next_inserter(self, segment): 
        raise NotImplemented()

class SegmentedInserter(InserterInterface):

    def __init__(self, segment_size=100000, segment_factory = None):
        pass
    
        self.segment = 1
        self.inserter = None
        self.count = 0
        self.segment_size = segment_size
        self.factory = segment_factory

        self.inserter = self.factory.next_inserter(self.segment)
       
        self.inserter.__enter__()
    
    def __enter__(self): 

        return self
            
    def __exit__(self, type_, value, traceback):
        self.inserter.__exit__(type_, value, traceback)
        return self
    
    def insert(self, row, **kwargs):
        
        self.count += 1
        
        if self.count > self.segment_size:
            self.segment += 1
            self.inserter = self.factory.next_inserter(self.segment)
            
            self.count = 0
        
        return self.inserter.insert(row)
        
    
    def close(self):
        self.inserter.close()


class ValueWriter(InserterInterface):
    '''Inserts arrays of values into  database table'''
    def __init__(self, bundle,  db, cache_size=50000, text_factory = None, replace=False, caster = None):
        import string 
        self.cache = []
        
        self.bundle = bundle
        self.db = db
        self.session = self.db.session
        self.connection = self.db.connection

        self.transaction = self.connection.begin()
        self.cache_size = cache_size
        self.statement = None
        self.caster = caster
        
        if text_factory:
            self.db.engine.raw_connection().connection.text_factory = text_factory

    def __enter__(self): 
        return self
        
    def rollback_end(self):
        if self.transaction:
            self.transaction.rollback()
            self.transaction = None
    
    def commit_end(self):
        if self.transaction:
            self.transaction.commit()
            self.transaction = None
        
    def commit_continue(self):
        if self.transaction:
            self.transaction.commit()
            self.transaction = self.connection.begin()
        else:
            print "NO TRANSACTION"
                
        
    def close(self):

        if len(self.cache) > 0 :       
            try:
                self.connection.execute(self.statement, self.cache)
                self.commit_end()
                self.cache = []
            except (KeyboardInterrupt, SystemExit):
                self.rollback_end()
                raise
            except Exception as e:
                if self.bundle:
                    self.bundle.error("Exception during ValueWriter.insert: "+str(e))
                self.rollback_end()
                raise
        else:
            self.commit_end()
                    
    def __exit__(self, type_, value, traceback):
    
        if type_ is not None:
            try: self.bundle.error("Got Exception: "+str(value))
            except:  print "ERROR: Got Exception {}: {}".format(type_, str(value))
            return False

        self.close()
           
                
        return self
        
 
class ValueInserter(ValueWriter):
    '''Inserts arrays of values into  database table'''
    def __init__(self, bundle, table, db, cache_size=50000, text_factory = None, replace=False, caster = None, skip_none=True): 
        
        super(ValueInserter, self).__init__(bundle, db, cache_size=cache_size, text_factory = text_factory, caster = caster)  
   
        self.table = table
        
        self.header = [c.name for c in self.table.columns]
   
        self.statement = self.table.insert()
        
        self.skip_none = skip_none
        
        self.null_row = self.bundle.schema.table(table.name).null_dict
    
        if replace:
            self.statement = self.statement.prefix_with('OR REPLACE')

    def insert(self, values):
      
        try:
            if isinstance(values, dict):
                if self.caster:
                    d = self.caster(values)
                else:
                    d = dict(values)
    
                if self.skip_none:
            
                    d = { k: d[k] if k in d and d[k] is not None else v for k,v in self.null_row.items() }
                  
                    
            else:
                
                if self.caster:
                    try:
                        d = self.caster(values)
                    except Exception as e:
                        raise ValueError("Failed to cast row: {}: {}".format(values, str(e)))
                else:
                    d = values

                d  = dict(zip(self.header, d))
                
                if self.skip_none:
                    d = { k: d[k] if k in d and d[k] is not None else v for k,v in self.null_row.items() }
                
            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size: 
                self.connection.execute(self.statement, self.cache)
                self.cache = []
                
                self.commit_continue()

        except (KeyboardInterrupt, SystemExit):
            if self.bundle:
                self.bundle.log("Processing keyboard interrupt or system exist")
            else:
                print "Processing keyboard interrupt or system exist" 
            self.rollback_end()
            self.cache = []
            raise
        except Exception as e:
            if self.bundle:
                self.bundle.error("Exception during ValueInserter.insert: {}".format(e))
            else:
                print "ERROR: Exception during ValueInserter.insert: {}".format(e)
            self.rollback_end()
            self.cache = []
            raise

        return True
   
class ValueUpdater(ValueWriter, UpdaterInterface):
    '''Updates arrays of values into  database table'''
    def __init__(self, bundle, table, db,  cache_size=50000, text_factory = None): 
        
        from sqlalchemy.sql.expression import bindparam, and_
        super(ValueUpdater, self).__init__(bundle, db, cache_size=50000, text_factory = text_factory)  
    
        self.table = table
        self.statement = self.table.update()
     
        wheres = []
        for primary_key in table.primary_key:
            wheres.append(primary_key == bindparam('_'+primary_key.name))
            
        if len(wheres) == 1:
            self.statement = self.statement.where(wheres[0])
        else:
            self.statement = self.statement.where(and_(wheres))
       
        self.values = None
       

    def update(self, values):
        from sqlalchemy.sql.expression import bindparam
        
        if not self.values:
            names = values.keys()
            
            binds = {}
            for col_name in names:
                if not col_name.startswith("_"):
                    raise ValueError("Columns names must start with _ for use in updater")
                
                column = self.table.c[col_name[1:]]
                binds[column.name] = bindparam(col_name)
                
                self.statement = self.statement.values(**binds)
       
        try:
            if isinstance(values, dict):
                d = values
            else:
                d  = dict(zip(self.header, values))
         
            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size:
                
                self.connection.execute(self.statement, self.cache)
                self.cache = []
                
        except (KeyboardInterrupt, SystemExit):
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise
        except Exception as e:
            self.bundle.error("Exception during ValueUpdater.insert: "+str(e))
            self.transaction.rollback()
            self.transaction = None
            self.cache = []
            raise e

        return True    

