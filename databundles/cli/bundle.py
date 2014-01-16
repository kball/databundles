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
    import os
    from ..run import import_file

    bundle_file = os.path.abspath(args.bundle_file)

    if not os.path.exists(bundle_file):
        err("Bundle code file does not exist: {}".format(bundle_file) )

    bundle_dir = os.path.dirname(bundle_file)

    config_file = os.path.join(bundle_dir, 'bundle.yaml')

    if not os.path.exists(config_file):
        err("Bundle config file does not exist: {}".format(bundle_file) )

    # Import the bundle file from the
    rp = os.path.realpath(bundle_file)
    mod = import_file(rp)

    dir_ = os.path.dirname(rp)
    b = mod.Bundle(dir_)

    def getf(f):
        return globals()['bundle_'+f]

    ph = {
          'meta': ['clean'],
          'prepare': ['clean'],
          'build' : ['clean', 'prepare'],
          'update' : ['clean', 'prepare'],
          'install' : ['clean', 'prepare', 'build'],
          'submit' : ['clean', 'prepare', 'build'],
          'extract' : ['clean', 'prepare', 'build']
          }

    phases = []

    if hasattr(args,'clean') and args.clean:
        # If the clean arg is set, then we need to run  clean, and all of the
        # earlier build phases.

        phases += ph[args.subcommand]

    phases.append(args.subcommand)

    for phase in phases:
        getf(phase)(args, b, rc)

def bundle_parser(cmd):
    import argparse, multiprocessing
    
    parser = cmd.add_parser('bundle', help='Manage bundle files')
    parser.set_defaults(command='bundle')
    parser.add_argument('-l','--library',  default='default',  help='Select a different name for the library')
    parser.add_argument('-f','--bundle-file', required=True,   help='Path to the bundle .py file')
    parser.add_argument('-t','--test',  default=False, action="store_true", help='Enable bundle-specific test behaviour')
    parser.add_argument('-m','--multi',  type = int,  nargs = '?',
                        default = 1,
                        const = multiprocessing.cpu_count(),
                        help='Run the build process on multiple processors, if the  method supports it')
    
    # These are args that Aptana / PyDev adds to runs. 
    parser.add_argument('--port', default=None, help="PyDev Debugger arg")
    parser.add_argument('--verbosity', default=None, help="PyDev Debugger arg")
    
    sub_cmd = parser.add_subparsers(title='commands', help='command help')

    command_p = sub_cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(subcommand='config')
       
    asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundl file')


    #
    # rewrite Command
    #

    sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
    sp.set_defaults(subcommand='rewrite')

    #
    # Dump Command
    #

    sp = asp.add_parser('dump', help='dump the configuration')     
    sp.set_defaults(subcommand='dump')

    #
    # Schema Command
    #

    sp = asp.add_parser('schema', help='Print the schema')     
    sp.set_defaults(subcommand='schema') 

    #
    # info command
    #
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-s','--schema',  default=False,action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')


    #
    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.set_defaults(subcommand='clean')
    
    #
    # Meta Command
    #
    command_p = sub_cmd.add_parser('meta', help='Build or install metadata')
    command_p.set_defaults(subcommand='meta')
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')     
                     
    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-r','--rebuild', default=False,action="store_true", help='Rebuild the schema, but dont delete built files')
    
    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    
    command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')
    
    
    
    #
    # Update Command
    #
    command_p = sub_cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(subcommand='update')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    
    
    #
    # Extract Command
    #
    command_p = sub_cmd.add_parser('extract', help='Extract data into CSV and TIFF files. ')
    command_p.set_defaults(subcommand='extract')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    
    #
    # Submit Command
    #
    command_p = sub_cmd.add_parser('submit', help='Submit extracts to the repository ')
    command_p.set_defaults(subcommand='submit')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')   
    command_p.add_argument('-r','--repo',  default=None, help='Name of the repository, defined in the config file')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    #
    # Install Command
    #
    command_p = sub_cmd.add_parser('install', help='Install bundles and partitions to the library')
    command_p.set_defaults(subcommand='install')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-l','--library',  help='Name of the library, defined in the config file')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Force storing the file')
    
    
    #
    # run Command
    #
    command_p = sub_cmd.add_parser('run', help='Run a method on the bundle')
    command_p.set_defaults(subcommand='run')
    command_p.add_argument('method', metavar='Method', type=str, 
                   help='Name of the method to run')    
    command_p.add_argument('args',  nargs='*', type=str,help='additional arguments')
    

     
    #
    # repopulate
    #
    command_p = sub_cmd.add_parser('repopulate', help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(subcommand='repopulate')
    
    
    #
    # Source Commands
    #
    
    command_p = sub_cmd.add_parser('commit', help='Commit the source')
    command_p.set_defaults(subcommand='commit', command_group='source')
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = sub_cmd.add_parser('push', help='Commit and push to the git origin')
    command_p.set_defaults(subcommand='push', command_group='source')
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = sub_cmd.add_parser('pull', help='Pull from the git origin')
    command_p.set_defaults(subcommand='pull', command_group='source')


def bundle_info(args, b, rc):
    if args.schema:
        print b.schema.as_csv()
    else:
        b.log("----Info ---")
        b.log("VID  : "+b.identity.vid)
        b.log("Name : "+b.identity.sname)
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



def bundle_clean(args, b, rc):
    b.log("---- Cleaning ---")
    # Only clean the meta phases when it is explicityly specified.
    #b.clean(clean_meta=('meta' in phases))
    b.clean()

def bundle_meta(args, b, rc):

    # The meta phase does not require a database, and should write files
    # that only need to be done once.
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

def bundle_prepare(args, b, rc):
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

def bundle_build(args, b, rc):

    if b.pre_build():
        b.log("---- Build ---")
        if b.build():
            b.post_build()
            b.log("---- Done Building ---")
        else:
            b.log("---- Build exited with failure ---")
            return False
    else:
        b.log("---- Skipping Build ---- ")

    return True

def bundle_install(args, b, rc):

    force = args.force

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

def bundle_run(args, b, rc):

    #
    # Run a method on the bundle. Can be used for testing and development.
    try:
        f = getattr(b,str(args.method))
    except AttributeError as e:
        b.error("Could not find method named '{}': {} ".format(args.method, e))
        b.error("Available methods : {} ".format(dir(b)))

        return

    if not callable(f):
        raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))

    return f(*args.args)

def bundle_submit(args, b, rc):

    if b.pre_submit():
        b.log("---- Submit ---")
        if b.submit():
            b.post_submit()
            b.log("---- Done Submitting ---")
        else:
            b.log("---- Submit exited with failure ---")
    else:
        b.log("---- Skipping Submit ---- ")

def bundle_extract(args, b, rc):
    if b.pre_extract():
        b.log("---- Extract ---")
        if b.extract():
            b.post_extract()
            b.log("---- Done Extracting ---")
        else:
            b.log("---- Extract exited with failure ---")
    else:
        b.log("---- Skipping Extract ---- ")

def bundle_update(args, b, rc):

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

def bundle_config(args, b, rc):

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

def bundle_source(args, b, rc):

    if 'command_group' in args and args.command_group == 'source':

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
