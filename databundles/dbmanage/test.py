"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbmanage import prt
                    
def test_command(args,rc, src):
    
    if args.subcommand == 'config':
        prt(rc.dump())
    elif args.subcommand == 'spatialite':
        from pysqlite2 import dbapi2 as db
        import os
        
        f = '/tmp/_db_spatialite_test.db'
        
        if os.path.exists(f):
            os.remove(f)
        
        conn = db.connect(f)
    
        cur = conn.cursor()
        
        try:
            conn.enable_load_extension(True)
            conn.execute("select load_extension('/usr/lib/libspatialite.so')")
            #loaded_extension = True
        except AttributeError:
            #loaded_extension = False
            prt("WARNING: Could not enable load_extension(). ")
        
        rs = cur.execute('SELECT sqlite_version(), spatialite_version()')

        for row in rs:
            msg = "> SQLite v%s Spatialite v%s" % (row[0], row[1])
            print(msg)

    
    else:
        prt('Testing')
        prt(args)