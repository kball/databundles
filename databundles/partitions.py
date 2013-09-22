"""Access classess and identity for partitions. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os

from partition import PartitionIdentity
from sqlalchemy.orm.exc import NoResultFound

class Partitions(object):
    '''Continer and manager for the set of partitions. 
    
    This object is always accessed from Bundle.partitions""
    '''
    
    def __init__(self, bundle):
        self.bundle = bundle

    def partition(self, arg,  **kwargs):
        '''Get a local partition object from either a Partion ORM object, or
        a partition name
        
        Arguments:
        arg    -- a orm.Partition or Partition object. 
        
        '''

        from databundles.orm import Partition as OrmPartition
        from databundles.identity import PartitionNumber
        from partition import PartitionIdentity
        from sqlalchemy import or_
        
        from partition import new_partition
        
        session = self.bundle.database.session

        if isinstance(arg,OrmPartition):
            orm_partition = arg
            
        elif isinstance(arg, basestring):
      
            orm_partition = session.query(OrmPartition).filter(or_(OrmPartition.id_==arg,OrmPartition.vid==arg)).one()

        elif isinstance(arg, PartitionNumber):      
            orm_partition = session.query(OrmPartition).filter(OrmPartition.id_==str(arg) ).one()
            
        elif isinstance(arg, PartitionIdentity):      
            orm_partition = session.query(OrmPartition).filter(OrmPartition.id_==str(arg.id_) ).one()  
               
        else:
            raise ValueError("Arg must be a Partition or PartitionNumber")

        if session.dirty:
            session.merge(orm_partition)


        return new_partition(self.bundle, orm_partition, **kwargs)


    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
    
        return self.bundle.database.session.query(OrmPartition).count()
    
    @property 
    def all(self): #@ReservedAssignment
        '''Return an iterator of all partitions'''
        from databundles.orm import Partition as OrmPartition
        import sqlalchemy.exc
        try:
            return [self.partition(op) for op in self.bundle.database.session.query(OrmPartition).all()]
        except sqlalchemy.exc.OperationalError:
            raise
            return []
            
        
    def __iter__(self):
        return iter(self.all)

            

    
    def get(self, id_):
        '''Get a partition by the id number 
        
        Arguments:
            id_ -- a partition id value
            
        Returns:
            A partitions.Partition object
            
        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique
            
        Because this method works on the bundle, it the id_ ( without version information )
        is equivalent to the vid ( with version information )
            
        ''' 
        from databundles.orm import Partition as OrmPartition
        from sqlalchemy import or_
        

        if isinstance(id_, PartitionIdentity):
            id_ = id_.identity.id_
     
        s = self.bundle.database.session
        
        q = (s
             .query(OrmPartition)
             .filter(or_(
                         OrmPartition.id_==str(id_).encode('ascii'),
                          OrmPartition.vid==str(id_).encode('ascii')
                         )))
  
        try:
            orm_partition = q.one()
          
            return self.partition(orm_partition)
        except NoResultFound:
            orm_partition = None
            
        if not orm_partition:
            q = (s.query(OrmPartition)
             .filter(OrmPartition.name==id_.encode('ascii')))

            try:
                orm_partition = q.one()
              
                return self.partition(orm_partition)
            except NoResultFound:
                orm_partition = None
            
        return orm_partition

    def find_table(self, table_name):
        '''Return the first partition that has the given table name'''
        
        for partition in self.all:
            if partition.table and partition.table.name == table_name:
                return partition
            
        return None

    def find(self, pid=None, use_library=False, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        import sqlalchemy.orm.exc
        from identity import Identity
        
        try:
            
            if pid and not pid.format:
                    pid.format = Identity.ANY
            elif not 'format' in kwargs:
                    kwargs['format'] = Identity.ANY
                
            partitions = [ self.partition(op, memory=kwargs.get('memory',False)) 
                          for op in self._find_orm(pid, **kwargs).all()];

            if len(partitions) == 1:
                p =  partitions.pop()
                
                if use_library and not p.database.exists:
                    # Try to get it from the library, if it exists. 
                    b = self.bundle.library.get(p.identity.vname)
                    
                    if not b or not b.partition:
                        return p
                    else:
                        return b.partition
                else:                   
                    return p
                    
                    
            elif len(partitions) > 1 :
                from databundles.dbexceptions import ResultCountError
                
                rl = "; ".join([p.identity.name for p in partitions])
                
                raise ResultCountError("Got too many results: {}".format(rl)) 
            else:
                return None
            
        except sqlalchemy.orm.exc.NoResultFound: 
            return None
   
    
    def find_all(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        from identity import Identity
        
        if pid and not pid.format:
                pid.format = Identity.ANY
        elif not 'format' in kwargs:
                kwargs['format'] = Identity.ANY

        ops = self._find_orm(pid, **kwargs).all()
        
        return [ self.partition(op) for op in ops]

    def _pid_or_args_to_pid(self, bundle,  pid, args):
        from databundles.identity import Identity, new_identity
        

        if isinstance(pid, Identity):
            return pid, None
        elif isinstance(pid,basestring):
            return None, pid # pid is actually the name
        elif args.get('name', False):
            return None, args.get('name', None)
        else:
            return new_identity(args, bundle=bundle), None

    
    def  _find_orm(self, pid=None,  **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        An ORM object is returned, so changes can be persisted. '''
        import sqlalchemy.orm.exc
        from databundles.identity import Identity
        from databundles.orm import Partition as OrmPartition
        
        pid, name = self._pid_or_args_to_pid(self.bundle, pid, kwargs)

        q =  self.bundle.database.session.query(OrmPartition)
        
        if name is not None:
            q = q.filter(OrmPartition.name==name)
        else:       
            if pid.time is not Identity.ANY:
                q = q.filter(OrmPartition.time==pid.time)
    
            if pid.space is not Identity.ANY:
                    q = q.filter(OrmPartition.space==pid.space)
        
            if pid.grain is not Identity.ANY:
                q = q.filter(OrmPartition.grain==pid.grain)
       
            if pid.format is not Identity.ANY:
                q = q.filter(OrmPartition.format==pid.format)

            if pid.segment is not Identity.ANY:
                q = q.filter(OrmPartition.segment==pid.segment)
       
        
            if pid.table is not Identity.ANY:
            
                if pid.table is None:
                    q = q.filter(OrmPartition.t_id==None)
                else:    
                    tr = self.bundle.schema.table(pid.table)
                    
                    if not tr:
                        raise ValueError("Didn't find table named {} in {} bundle path = {}".format(pid.table, pid.vname, self.bundle.database.path))
                    
                    q = q.filter(OrmPartition.t_id==tr.id_)
 
        return q

    def new_orm_partition(self, pid,  **kwargs):
        '''Create a new ORM Partrition object, or return one if
        it already exists '''
        from databundles.orm import Partition as OrmPartition, Table
     
        session = self.bundle.database.session

        if pid.table:
            q =session.query(Table).filter( (Table.name==pid.table) |  (Table.id_==pid.table) )
            table = q.one()
        else:
            table = None
     
        # 'tables' are additional tables that are part of the partion ,beyond the one in the identity
        # Probably a bad idea. 
        tables = kwargs.get('tables',kwargs.get('table',pid.table if pid else None))
    
        if tables and not isinstance(tables, (list,tuple)):
            tables = [tables]
    
        if tables and pid and pid.table and pid.table not in tables:
            tables = list(tables)
            tables.append(pid.table)
         
        data=kwargs.get('data',{})
        
        data['tables'] = tables

        d = pid.to_dict()
        
        if not 'format' in d:
            d['format']  = kwargs.get('format', 'db')
        
        try: del d['table'] # OrmPartition requires t_id instead
        except: pass

        if 'dataset' in d:
            del d['dataset']
         
        # This code must have the session established in the context be active. 
        op = OrmPartition(
                self.bundle.get_dataset(session),         
                t_id = table.id_ if table else None,
                data=data,
                state=kwargs.get('state',None),
                **d
             )  
        
        self.bundle.database._session.commit()
        
        return op

    def clean(self, session):
        from databundles.orm import Partition as OrmPartition
   
        session.query(OrmPartition).delete()
        
    def _new_partition(self, pid=None, session = None,**kwargs):
        '''Creates a new OrmPartition record'''
        
        pid, _ = self._pid_or_args_to_pid(self.bundle, pid, kwargs)

        extant = self._find_orm(pid, **kwargs).all()
        
        for p in extant:
            if p.name == pid.name:
                return self.partition(p)
       
        op = self.new_orm_partition(pid, **kwargs)
        
        self.bundle.database.session.add(op)   
        self.bundle.database.session.commit()

        p = self.partition(op,  **kwargs)
        return p


    def new_partition(self, pid=None, **kwargs):
        return self.new_db_partition( pid, **kwargs)

    def new_db_partition(self, pid=None, **kwargs):
        
        if pid:
            pid.format = 'db'
        else: 
            kwargs['format'] = 'db'
            
        return self._new_partition(pid, **kwargs)
    
    def new_geo_partition(self, pid=None, **kwargs):
        
        if pid:
            pid.format = 'geo'
        else: 
            kwargs['format'] = 'geo'
        
        # We'll need to load a table from the shapefile, so that has to be created before
        # we create the partition. 
        table_name = kwargs.get('table',pid.table if pid else None)
        
        if not table_name:
            raise ValueError("Pid must have a table name")

        if not self.bundle.schema.table(table_name):
            self.bundle.schema.add_table(table_name)

        p = self._new_partition(pid, **kwargs)

        if kwargs.get('shape_file'):
            p.load_shapefile( kwargs.get('shape_file'), **kwargs)
     
        return p
        
    def new_hdf_partition(self, pid=None, **kwargs):
        
        if pid:
            pid.format = 'hdf'
        else: 
            kwargs['format'] = 'hdf'
            
        return self._new_partition(pid, **kwargs)
        
    def new_csv_partition(self, pid=None, **kwargs):
        
        if pid:
            pid.format = 'csv'
        else: 
            kwargs['format'] = 'csv'
        
        return self._new_partition(pid, **kwargs)
        
    def find_or_new(self, pid=None, clean = False,  **kwargs):
        return self.find_or_new_db(pid, clean = False,  **kwargs)

    def find_or_new_db(self, pid=None, clean = False,  **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''
        
        if pid:
            pid.format = 'db'
        else: 
            kwargs['format'] = 'db'
        

        try: partition =  self.find(pid **kwargs)
        except: partition = None
    
        if partition:
            return partition
    
        tables = kwargs.get('tables',kwargs.get('table',pid.table if pid else None))
    
        if tables and not isinstance(tables, (list,tuple)):
            tables = [tables]
    
        if tables and pid and pid.table and pid.table not in tables:
            tables.append(partition.identity.table)
   
        partition =  self._new_partition(pid, **kwargs)
        
        if tables:   
            partition.create_with_tables(tables, clean)  
        else:
            partition.create()

        return partition;
    
    def find_or_new_geo(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''
        
        if pid:
            pid.format = 'geo'
        else: 
            kwargs['format'] = 'geo'
        
        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition

        tables = kwargs.get('tables',kwargs.get('table',pid.table if pid else None))
    
        if tables and not isinstance(tables, (list,tuple)):
            tables = [tables]
    
        if tables and pid and pid.table and pid.table not in tables:
            tables.append(partition.identity.table)

        partition = self.new_geo_partition(pid, **kwargs)

        if tables:   
            partition.create_with_tables(tables)  
        else:
            partition.create()

        return partition;
    
    def find_or_new_hdf(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''

        if pid:
            pid.format = 'hdf'
        else: 
            kwargs['format'] = 'hdf'

        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition

        partition = self.new_hdf_partition(pid, **kwargs)

        return partition;

    def find_or_new_csv(self, pid=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''

        if pid:
            pid.format = 'csv'
        else: 
            kwargs['format'] = 'csv'

        try: partition =  self.find(pid, **kwargs)
        except: partition = None
    
        if partition:
            return partition

        partition = self.new_csv_partition(pid, **kwargs)

        return partition;

    def delete(self, partition):
        from databundles.orm import Partition as OrmPartition
 
        q = (self.bundle.database.session.query(OrmPartition)
             .filter(OrmPartition.id_==partition.identity.id_))
      
        q.delete()

