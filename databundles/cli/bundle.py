"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""



from ..cli import prt, err, warn
from ..cli import _library_list, _source_list, load_bundle, _print_bundle_list

import os
import yaml
import shutil

def bundle_command(args, rc, src):

    globals()['bundle_'+args.subcommand](args, rc,src)

def bundle_parser(cmd):
    import argparse, multiprocessing
    
    bundle_p = cmd.add_parser('bundle', help='Manage bundle files')
    bundle_p.set_defaults(command='bundle')
    bundle_p.add_argument('-l','--library',  default='default',  help='Select a different name for the library')
    bundle_p.add_argument('-f','--bundle-file', required=True,   help='Path to the bundle .py file')

    asp = bundle_p.add_subparsers(title='bundle commands', help='command help')  

    sp = asp.add_parser('info', help='Information about the bundle configuration')
    sp.set_defaults(subcommand='info')

    parser = None

    # Commands: meta, prepare, build, install, extract, submit, 
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
    
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file")
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose")
    parser.add_argument('-r','--reset',  default=False, action="store_true",  help='')
    parser.add_argument('-t','--test',  default=False, action="store_true", help='Enable bundle-specific test behaviour')
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")
    
    parser.add_argument('-m','--multi',  type = int,  nargs = '?',
                        default = 1,
                        const = multiprocessing.cpu_count(),
                        help='Run the build process on multiple processors, if the  method supports it')
    
    # These are args that Aptana / PyDev adds to runs. 
    parser.add_argument('--port', default=None, help="PyDev Debugger arg")
    parser.add_argument('--verbosity', default=None, help="PyDev Debugger arg")
    
    cmd = parser.add_subparsers(title='commands', help='command help')

    command_p = cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(command='config')
       
    asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundl file')

    sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
    sp.set_defaults(subcommand='rewrite')
    
    sp = asp.add_parser('dump', help='dump the configuration')     
    sp.set_defaults(subcommand='dump') 
    
    sp = asp.add_parser('schema', help='Print the schema')     
    sp.set_defaults(subcommand='schema') 
    
    #
    # Clean Command
    #
    command_p = cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.set_defaults(command='clean')   
    
    #
    # Meta Command
    #
    command_p = cmd.add_parser('meta', help='Build or install metadata')
    command_p.set_defaults(command='meta')   
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')     
                     
    #
    # Prepare Command
    #
    command_p = cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(command='prepare')   
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-r','--rebuild', default=False,action="store_true", help='Rebuild the schema, but dont delete built files')
    
    #
    # Build Command
    #
    command_p = cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(command='build')   
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    
    command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')
    
    
    
    #
    # Update Command
    #
    command_p = cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(command='update')   
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    
    
    #
    # Extract Command
    #
    command_p = cmd.add_parser('extract', help='Extract data into CSV and TIFF files. ')
    command_p.set_defaults(command='extract')   
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    
    #
    # Submit Command
    #
    command_p = cmd.add_parser('submit', help='Submit extracts to the repository ')
    command_p.set_defaults(command='submit')    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')   
    command_p.add_argument('-r','--repo',  default=None, help='Name of the repository, defined in the config file')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    #
    # Install Command
    #
    command_p = cmd.add_parser('install', help='Install bundles and partitions to the library')
    command_p.set_defaults(command='install')  
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-l','--library',  help='Name of the library, defined in the config file')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Force storing the file')
    
    
    #
    # run Command
    #
    command_p = cmd.add_parser('run', help='Run a method on the bundle')
    command_p.set_defaults(command='run')               
    command_p.add_argument('method', metavar='Method', type=str, 
                   help='Name of the method to run')    
    command_p.add_argument('args',  nargs='*', type=str,help='additional arguments')
    
    #
    # info command
    #
    command_p = cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(command='info')               
    command_p.add_argument('-s','--schema',  default=False,action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')
     
    #
    # repopulate
    #
    command_p = cmd.add_parser('repopulate', help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(command='repopulate')               
    
    
    #
    # Source Commands
    #
    
    command_p = cmd.add_parser('commit', help='Commit the source')
    command_p.set_defaults(command='commit', command_group='source')  
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = cmd.add_parser('push', help='Commit and push to the git origin')
    command_p.set_defaults(command='push', command_group='source')  
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = cmd.add_parser('pull', help='Pull from the git origin')
    command_p.set_defaults(command='pull', command_group='source')  


    def run_prepare(self):
        b = self
        if b.pre_prepare():
            b.log("---- Preparing ----")
            if b.prepare():
                b.post_prepare()
                b.log("---- Done Preparing ----")
            else:
                b.log("---- Prepare exited with failure ----")
                return False
        else:
            b.log("---- Skipping prepare ---- ")

        return True

    def run_install(self, force=False):
        b = self
        if b.pre_install():
            b.log("---- Install ---")
            if b.install(force=force):
                b.post_install()
                b.log("---- Done Installing ---")
            else:
                b.log("---- Install exited with failure ---")
                return False
        else:
            b.log("---- Skipping Install ---- ")
                
        return True
                
    def run(self, argv):

        b = self
        args =  b.parse_args(argv)
    
        if args.command == 'config':
            if args.subcommand == 'rewrite':
                b.log("Rewriting the config file")
                with self.session:
                    b.update_configuration()
            elif args.subcommand == 'dump':
                print b.config._run_config.dump()
            elif args.subcommand == 'schema':
                print b.schema.as_markdown()
            return
    
        if 'command_group' in args and args.command_group == 'source':
            
            from source.repository import new_repository
    
            repo = new_repository(b.config._run_config.sourcerepo('default'))   
            repo.bundle = b

            if args.command == 'commit':
                repo.commit(args.message)
            elif args.command == 'push':
                repo.commit(args.message)
                repo.push()
            elif args.command == 'pull':
                repo.pull()
            
            return 
    
        if args.command == 'repopulate':
            b.repopulate()
            return 
    
        if hasattr(args,'clean') and args.clean:
            # If the clean arg is set, then we need to run  clean, and all of the
            # earlerier build phases. 
            ph = {
                  'meta': ['clean'],
                  'prepare': ['clean'],
                  'build' : ['clean', 'prepare'],
                  'update' : ['clean', 'prepare'],
                  'install' : ['clean', 'prepare', 'build'],
                  'submit' : ['clean', 'prepare', 'build'],
                  'extract' : ['clean', 'prepare', 'build']
                  }
    
        else:
            ph = {
                  'build' : [ 'prepare'],
                  }
    
        phases = ph.get(args.command,[]) + [args.command]
    
        if args.test:
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "!!!!!! In Test Mode !!!!!!!!!!"
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            import time
            time.sleep(1)
    
        if 'info' in phases:
            if args.schema:
                print b.schema.as_csv()
            else:
                b.log("----Info ---")
                b.log("VID  : "+b.identity.vid)
                b.log("Name : "+b.identity.name)
                b.log("VName: "+b.identity.vname)
                b.log("Parts: {}".format(b.partitions.count))
                
                if b.config.build.get('dependencies',False):
                    b.log("---- Dependencies ---")
                    for k,v in b.config.build.dependencies.items():
                        b.log("    {}: {}".format(k,v))

                if b.partitions.count < 5:
                    b.log("---- Partitions ---")
                    for partition in b.partitions:
                        b.log("    "+partition.name)
                
            return
        
        if 'run' in phases:
            #
            # Run a method on the bundle. Can be used for testing and development. 
            try:
                f = getattr(b,str(args.method))
            except AttributeError as e:
                b.error("Could multinot find method named '{}': {} ".format(args.method, e))
                b.error("Available methods : {} ".format(dir(b)))
          
                return
            
            if not callable(f):
                raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))
          
            return f(*args.args)
           
        
    
        if 'clean' in phases:
            b.log("---- Cleaning ---")
            # Only clean the meta phases when it is explicityly specified. 
            b.clean(clean_meta=('meta' in phases))
            
        # The Meta phase prepares neta information, such as list of cites
        # that is doenloaded from a website, or a specificatoin for a schema. 
        # The meta phase does not require a database, and should write files
        # that only need to be done once. 
        if 'meta' in phases:
            if b.pre_meta():
                b.log("---- Meta ----")
                if b.meta():
                    b.post_meta()
                    b.log("---- Done Meta ----")
                else:
                    b.log("---- Meta exited with failure ----")
                    return False
            else:
                b.log("---- Skipping Meta ---- ")

                   
            
        if 'prepare' in phases:
            if not b.run_prepare():
                return False

        if 'build' in phases:
            
            if b.run_args.test:
                print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                print "!!!!!! In Test Mode !!!!!!!!!!"
                print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    
                time.sleep(1)
                
            if not b.run_build():
                return False
                

        if 'update' in phases:
                
            if b.pre_update():
                b.log("---- Update ---")
                if b.update():
                    b.post_update()
                    b.log("---- Done Updating ---")
                else:
                    b.log("---- Update exited with failure ---")
                    return False
            else:
                b.log("---- Skipping Update ---- ")

        if 'install' in phases:
            self.run_install()


        if 'extract' in phases:
            if b.pre_extract():
                b.log("---- Extract ---")
                if b.extract():
                    b.post_extract()
                    b.log("---- Done Extracting ---")
                else:
                    b.log("---- Extract exited with failure ---")
            else:
                b.log("---- Skipping Extract ---- ")

        # Submit puts information about the the bundles into a catalog
        # and may store extracts of the data in the catalog. 
        if 'submit' in phases:
            if b.pre_submit():
                b.log("---- Submit ---")
                if b.submit():
                    b.post_submit()
                    b.log("---- Done Submitting ---")
                else:
                    b.log("---- Submit exited with failure ---")
            else:
                b.log("---- Skipping Submit ---- ")
       
        if 'test' in phases:
            ''' Run the unit tests'''
            import nose, unittest, sys  # @UnresolvedImport
    
            dir_ = b.filesystem.path('test') #@ReservedAssignment
                             
                       
            loader = nose.loader.TestLoader()
            tests =loader.loadTestsFromDir(dir_)
            
            result = unittest.TextTestResult(sys.stdout, True, 1) #@UnusedVariable
            
            print "Loading tests from ",dir_
            for test in tests:
                print "Running ", test
                test.context.bundle = b
                unittest.TextTestRunner().run(test)

             
            