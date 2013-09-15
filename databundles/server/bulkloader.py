"""Common exception objects

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__version__ = 1

import os
from ..util import get_logger


class Context(object):
    
    def __init__(self, fifos):
        
        self.fifos = fifos
    
    def __enter__(self):
        pass
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        
        for f,path in self.fifos:
            print "Removing",f
            f.close()
            if os.path.exists(path):
                os.remove(path)
        
        

class BulkLoader(object):
    '''Downloads CSV files from the web and loads them into a database, using the bulk loader features'''
    
    
    def __init__(self, prog_name, args, config, logger):
        self.lib_dir = '/var/lib/'+prog_name
        self.run_dir = '/var/run/'+prog_name
        self.log_dir = '/var/log/'+prog_name
        self.logger = logger

        self.fifos = []
        


    def mkfifo(self,name, mode):
        
        path = os.path.join(self.run_dir, name)
        
        if os.path.exists(path):
            os.remove(path)
        
        os.mkfifo(path, 0o0666)

        f = open(path, mode, 0)
        
        self.fifos.append((f,path))
        
        return f

    def run(self, ):
        import time

        with Context(self.fifos) as fc:

            while True:
                self.logger.info("Starting read loop")
                self.control_in = self.mkfifo('control_in', 'rb')
    
                for line in iter(self.control_in):
                    self.logger.info(line)
           
            #time.sleep(1)


def main():
    import argparse
    from databundles.run import  get_runconfig
    from databundles.util import daemonize
    
    parser = argparse.ArgumentParser(prog='bulkloader',
                                     description='Bulkloader, version {}'.format(__version__))
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
 
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")

    parser.add_argument('-d','--daemonize', default=False, action="store_true",   help="Run as a daemon") 
    parser.add_argument('-k','--kill', default=False, action="store_true",   help="With --daemonize, kill the running daemon process") 
    parser.add_argument('-L','--unlock', default=False, action="store_true",   help="Reclaim lockfile if it is locked") 
    parser.add_argument('-g','--group', default=None,   help="Set group for daemon operation") 
    parser.add_argument('-u','--user', default=None,  help="Set user for daemon operation")  
    parser.add_argument('-t','--test', default=False, action="store_true",   help="Run the test version of the server")   

    parser.add_argument('-D','--dir',  default='/var/run/bulkloader',  help='Directory to create fifos')

    args = parser.parse_args()

    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception("--single_config can only be specified with one -c")
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
            rc_path = args.config.pop()
    else:
        rc_path = args.config
    
    rc = get_runconfig(rc_path)
    
    prog_name='bulkloader'
    
    daemonize(run, args,  rc, prog_name=prog_name)
      
def run(prog_name, args, config, logger):

    bl = BulkLoader(prog_name, args, config, logger)
    
    try:
        bl.run()
    except Exception as e:
        logger.error(e)

        
if __name__ == '__main__':

        main()