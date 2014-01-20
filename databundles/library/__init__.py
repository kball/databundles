"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path

from databundles.util import temp_file_name
from databundles.dbexceptions import ConfigurationError, NotFoundError
from databundles.bundle import DbBundle

# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger.
import logging
import logging.handlers

libraries = {}

def _new_library(config):

    import copy
    from ..cache import new_cache, RemoteMarker
    from database import LibraryDb

    config = copy.deepcopy(config)

    #import pprint; pprint.pprint(config.to_dict())

    cache = new_cache(config['filesystem'])

    database = LibraryDb(**dict(config['database']))

    database.create()

    upstream = new_cache(config['upstream']) if 'upstream' in config else None

    config['name'] = config['_name'] if '_name' in config else 'NONE'

    for key in ['_name', 'filesystem', 'database', 'remote' ]:
        if key in config:
            del config[key]

    if upstream and (not isinstance(upstream, RemoteMarker)
                   and not isinstance(upstream.last_upstream(), RemoteMarker)):
        raise ConfigurationError("Library upstream must have a RemoteMarker interface: {}".format(config))

    if 'upstream' in config:
        del config['upstream']

    l =  Library(cache = cache,
                 database = database,
                 upstream = upstream,
                 **config)

    return l


def new_library(config, reset=False):
    """Return a new :class:`~databundles.library.Library`, constructed from a configuration

    :param config: a :class:`~databundles.run.RunConfig` object
    :rtype:  :class:`~databundles.library.Library`

    If ``config`` is None, the function will constuct a new :class:`~databundles.run.RunConfig` with a default
    constructor.

    """

    global libraries

    if reset:
        libraries = {}

    name = config['_name']

    if name is None:
        name = 'default'

    if name not in libraries:
        libraries[name] = _new_library(config)

    l =  libraries[name]
    l.clear_dependencies()

    return l


class Library(object):
    '''

    '''
    import collections

    def __init__(self, cache,database, name =None, upstream=None, sync=False, require_upload = False, host=None,port = None):
        '''
        Libraries are constructed on the root cache name for the library.
        If the cache does not exist, it will be created.

        Args:

            cache: a path name to a directory where bundle files will be stored
            database:
            remote: URL of a remote library, for fallback for get and put.
            sync: If true, put to remote synchronously. Defaults to False.

        '''

        self.name = name
        self.cache = cache
        self._database = database
        self._upstream = upstream
        self.sync = sync
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port
        self.dep_cb = None# Callback for dependency resolution
        self.require_upload = require_upload
        self._dependencies = None


        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = logging.getLogger(__name__)
        #self.logger.setLevel(logging.DEBUG)

        self.needs_update = False


    def clone(self):

        return self.__class__(self.cache, self.database.clone(), self._upstream, self.sync, self.require_upload, self.host, self.port)


    @property
    def upstream(self):
        if self._upstream:
            return self._upstream # When it is a URL to a REST interface.
        else:
            return None

    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return self._database

    def load(self, rel_path, decl_md5=None):
        '''Load a bundle from the remote to the local cache and install it'''
        from ..util.flo import copy_file_or_flo
        from  ..dbexceptions import ConflictError

        if not self.upstream.has(rel_path):
            raise ConfigurationError('Remote {} does not have cache key  {}'.format(self.upstream, rel_path))

        if not self.cache.has(rel_path):
            source = self.upstream.get_stream(rel_path)

            sink = self.cache.put_stream(rel_path, metadata=source.meta)

            try:
                copy_file_or_flo(source, sink)
            except:
                self.cache.remove(rel_path, propagate=True)
                raise

            source.close()
            sink.close()

        file_md5 = self.cache.md5(rel_path)

        if file_md5 != decl_md5:
            raise ConflictError('MD5 Mismatch: file={} != declared={} '.format( file_md5 , decl_md5))

        abs_path = self.cache.path(rel_path)
        b = DbBundle(abs_path)

        if b.identity.cache_key != rel_path:
            raise ConflictError("Identity of downloaded bundle doesn't match request payload")

        self.put(b)

        return b

    def config(self, bp_id):

        from ..cache import RemoteMarker

        d,p = self.get_ref(bp_id)

        try:
            api = self.upstream.get_upstream(RemoteMarker)
        except AttributeError: # No api
            api = self.upstream

        if self.cache.has(d.cache_key):
            b = self.get(d.vid)
            config = b.db_config.dict
        else:
            return None

    ##
    ## Storing
    ##

    def put(self, bundle, force=False):
        '''Install a single bundle or partition file into the library.

        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object

        '''
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        if not isinstance(bundle, (PartitionInterface, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")


        dst, cache_key, url = self._put_file(bundle.identity, bundle.database.path, force=force)

        return dst, cache_key, url


    def put_bundle(self, bundle, force=False):

        self.put(bundle, force=force)

        for p in bundle.partitions:
            self.put(p, force=force)


    def _put_file(self, identity, file_path, state='new', force=False):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity'''
        from ..identity import Identity
        if isinstance(identity , dict):
            identity = Identity.from_dict(identity)

        if not self.cache.has(identity.cache_key) or force:
            dst = self.cache.put(file_path,identity.cache_key)
        else:
            dst = self.cache.path(identity.cache_key)

        if not os.path.exists(dst):
            raise Exception("cache {}.put() didn't return an existent path. got: {}".format(type(self.cache), dst))

        if self.upstream and self.sync:
            self.upstream.put(identity, file_path)


        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ ='bundle')
        else:
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ = 'partition')

        return dst, identity.cache_key, self.cache.last_upstream().path(identity.cache_key)


    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.cache.remove(bundle.identity.cache_key, propagate = True)


    ##
    ## Retreiving
    ##


    def list(self, datasets=None, with_meta = True, key='vid'):
        '''Lists all of the datasets in the partition, optionally with
        metadata. Does not include partitions. This returns a dictionary
        in  a form that is similar to the remote and source lists. '''
        import socket
        from databundles.orm import Dataset, Config
        from ..identity import LocationRef, Identity


        if datasets is None:
            datasets = {}


        self.database.list(datasets=datasets)

        return sorted(datasets.values(), key=lambda x: x.vname)

    def path(self, rel_path):
        """Return the cache path for a cache key"""

        return self.cache.path(rel_path)


    def get(self,bp_id, force = False, cb=None):
        '''Get a bundle, given an id string or a name '''

        # Get a reference to the dataset, partition and relative path
        # from the local database.

        ip, dataset = self.database.resolver.resolve_ref_one(bp_id)

        if dataset:
            pass
        else:
            # We don't have the object in this database, so look for it elsewhere.
            pass



        if dataset and dataset.partition:
            return self._get_partition(dataset, dataset.partition, force, cb=cb)
        elif dataset:
            return self._get_dataset(dataset, force, cb=cb)
        else:
            return False


    def _get_remote_dataset(self, dataset, cb=None):

        from ..identity import Identity
        from ..util import copy_file_or_flo

        identity = Identity.from_dict(dataset.dict)

        source = self.upstream.get_stream(identity.cache_key)

        if not source:
            return False

        # Store it in the local cache.

        sink = self.cache.put_stream(identity.cache_key, metadata=source.meta)

        try:
            copy_file_or_flo(source, sink, cb=cb)
        except:
            self.cache.remove(identity.cache_key, propagate=True)
            raise

        abs_path = self.cache.path(identity.cache_key)

        #if not self.cache.has(identity.cache_key):
        #    abs_path = self.cache.put(r, identity.cache_key )
        #else:
        #    abs_path = self.cache.get(identity.cache_key )

        if not os.path.exists(abs_path):
            raise Exception("Didn't get file '{}' for id {}".format(abs_path, identity.cache_key))

        return abs_path

    def _get_remote_partition(self, bundle, partition, cb = None):

        from ..identity import  Identity
        from ..util import copy_file_or_flo

        identity = bundle.identity.as_partition(**partition.dict)

        p = bundle.partitions.get(identity.id_) # Get partition information from bundle

        if not p:
            from databundles.dbexceptions import NotFoundError
            raise NotFoundError("Failed to find partition {} in bundle {}"
                                .format(identity.name, bundle.identity.name))

        if os.path.exists(p.database.path) and not p.database.is_empty():

            from databundles.dbexceptions import ConflictError
            raise ConflictError("Trying to get {}, but file {} already exists".format(identity.vname, p.database.path))

        # Now actually get it from the remote.

        source = self.upstream.get_stream(p.identity.cache_key, return_meta=True)

        # Store it in the local cache.
        sink = self.cache.put_stream(p.identity.cache_key, metadata=source.meta)

        try:
            if cb:
                def progress_cb(i):
                    cb(i,source.meta['content-length'])
            else:
                progress_cb = None

            copy_file_or_flo(source, sink,cb=progress_cb)

        except:
            self.cache.remove(p.identity.cache_key, propagate = True)
            raise

        p_abs_path = self.cache.path(p.identity.cache_key)


        if os.path.realpath(p.database.path) != os.path.realpath(p_abs_path):
            m =( "Path mismatch in downloading partition: {} != {}"
                 .format(os.path.realpath(p.database.path),
                                os.path.realpath(p_abs_path)))

            self.logger.error(m)
            raise Exception(m)

        return p_abs_path, p

    def _get_dataset(self, dataset, force = False, cb=None):

        from sqlite3 import  DatabaseError

        # Try to get the file from the cache.
        abs_path = self.cache.get(dataset.cache_key, cb=cb)

        if not abs_path or not os.path.exists(abs_path):
            return False

        try:
            bundle = DbBundle(abs_path)
        except DatabaseError:
            self.logger.error("Failed to load databundle at path {}".format(abs_path))
            raise

        # Do we have it in the database? If not install it.
        # It should be installed if it was retrieved remotely,
        # but may not be installed if there is a local copy in the dcache.
        d = self.database.get(bundle.identity.vid)
        if not d:
            self.database.install_bundle(bundle)
            self.database.add_file(abs_path, self.cache.repo_id, bundle.identity.vid, 'pulled')

        bundle.library = self

        return bundle

    def _get_partition(self,  dataset, partition, force = False, cb=None):
        from databundles.dbexceptions import NotFoundError
        from databundles.client.exceptions import NotFound as RemoteNotFound

        r = self._get_dataset(dataset, cb=cb)

        if not r:
            return False

        try:
            p =  r.partitions.partition(partition)
        except:
            raise NotFoundError("Partition '{}' not in bundle  '{}' ".format(partition, r.identity.name ))

        rp = self.cache.get(p.identity.cache_key, cb=cb)


        if not rp or not os.path.exists(p.database.path) or p.database.is_empty() or force:

            if self.upstream:
                try:

                    if os.path.exists(p.database.path) and (force or p.database.is_empty()) :
                        self.logger.info("_get_partition deleting {} before fetch. force={}, is_empty={}"
                                    .format(p.database.path, force, p.database.is_empty()))


                        os.remove(p.database.path)

                    self._get_remote_partition(r,partition, cb=cb)
                except RemoteNotFound:
                    raise NotFoundError(("Didn't find partition {} in bundle {}. Partition found in bundle, "+
                    "but path {} ({}?) not in local library and doesn't have it either. ")
                                   .format(p.identity.name,r.identity.name,p.database.path, rp))
                except OSError:
                    raise NotFoundError("""Didn't find partition {} for bundle {}. Missing path {} ({}?) not found """
                                   .format(p.identity.name,r.identity.name,p.database.path, rp))

            else:
                raise NotFoundError(("Didn't  partition {}. Partition {} "+
                                    "found in bundle, but path {} ({}?) not in local "+
                                    "library and remote not set. force={}, is_empty={}")
                                    .format( p.identity.name,r.identity.name,
                                             p.database.path, rp,
                                             force, p.database.is_empty()))


            # Ensure the file is in the local library.

        ds = self.database.get(p.identity.vid)

        if not ds.partition:# It is a dataset, not a partition
            self.database.add_file(p.database.path, self.cache.repo_id, p.identity.vid, 'pulled')
            self.database.install_partition(p.bundle, p.identity)

        p.library = self

        # Unsetting the database is particularly necessary for Hdf partitions where the file will get loaded when the partition
        # is first created, which creates and empty file, and the empty file is carried forward after the file on
        # disk is changed.
        p.unset_database()
        r.partition = p

        return r

    ##
    ## Finding
    ##

    def find(self, query_command):

        return self.database.find(query_command)


    @property
    def resolver(self):
        return self.database.resolver

    def resolve(self, ref):

        ip, dataset =  self.resolver.resolve_ref_one(ref)

        if not dataset:
            return None

        if dataset.partition:
            return dataset.partition

        return dataset


    ##
    ## Dependencies
    ##

    @property

    def dep(self,name):
        """"Bundle version of get(), which uses a key in the
        bundles configuration group 'dependencies' to resolve to a name"""

        bundle_name = self.dependencies.get(name, False)

        if not bundle_name:
            raise ConfigurationError("No dependency named '{}'".format(name))

        b = self.get(bundle_name)

        if not b:
            raise NotFoundError("Failed to get dependency, key={}, id={}".format(name, bundle_name))


        if self.dep_cb:
            self.dep_cb( self, name, bundle_name, b)

        return b

    @property
    def dependencies(self):

        if not self._dependencies:
            self ._dependencies = self._get_dependencies()

        return self._dependencies

    def clear_dependencies(self):
        self._dependencies = None

    def _get_dependencies(self):
        from databundles.identity import Identity

        if not self.bundle:
            raise ConfigurationError("Can't use the dep() method for a library that is not attached to a bundle");


        group = self.bundle.config.group('build')

        try:
            deps = group.get('dependencies')
        except AttributeError:
            deps = None

        if not deps:
            return {}

        out = {}
        for k,v in deps.items():

            try:
                Identity.parse_name(v)
                out[k] = v
            except Exception as e:
                self.bundle.error("Failed to parse dependency name '{}' for '{}': {}".format(v, self.bundle.identity.name, e.message))

        return out

    def check_dependencies(self, throw=True):
        from ..util import Progressor
        errors = {}
        for k,v in self.dependencies.items():
            self.logger.info('Download and check dependency: {}'.format(v))
            b = self.get(v, cb=Progressor().progress)

            if not b:
                if throw:
                    raise NotFoundError("Dependency check failed for key={}, id={}".format(k, v))
                else:
                    errors[k] = v


    @property
    def new_files(self):
        '''Generator that returns files that should be pushed to the remote
        library'''

        new_files = self.database.get_file_by_state('new')

        for nf in new_files:
            yield nf

    def push(self, ref=None , cb=None):
        """Push any files marked 'new' to the upstream

        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files.

        """
        import time

        if not self.upstream:
            raise Exception("Can't push() without defining a upstream. ")

        if ref is not None:

            ip, dsid = self.resolver.resolve_ref_one(ref)

            if not dsid:
                raise Exception("Didn't get id from database for ref: {}".format(ref))

            if dsid.partition:
                identity = dsid.partition
            else:
                identity = dsid

            files = self.database.get_file_by_ref(identity.vid)
            file_ = files.pop(0)

            md = identity.to_meta(file=file_.path)

            if self.upstream.has(identity.cache_key):
                if cb: cb('Has',md,0)
                file_.state = 'pushed'
            else:
                start = time.clock()
                if cb: cb('Pushing',md, start)
                self.upstream.put(file_.path, identity.cache_key, metadata=md)
                file_.state = 'pushed'
                if cb: cb('Pushed',md, time.clock()-start)

            self.database.session.merge(file_)
            self.database.commit()
        else:
            for file_ in self.new_files:
                self.push(file_.ref, cb=cb)


    #
    # Maintainence
    #

    def clean(self, add_config_root=True):
        self.database.clean(add_config_root=add_config_root)


    def purge(self):
        """Remove all records from the library database, then delete all
        files from the cache"""
        self.clean()
        self.cache.clean()

    #
    # Backup and restore
    #

    def run_dumper_thread(self):
        '''Run a thread that will check the database and call the callback when the database should be
        backed up after a change. '''
        from util import DumperThread
        dt = DumperThread(self.clone())
        dt.start()

        return dt

    def backup(self):
        '''Backup the database to the remote, but only if the database needs to be backed up. '''


        if not self.database.needs_dump():
            return False

        backup_file = temp_file_name()+".db"

        self.database.dump(backup_file)

        path = self.upstream.put(backup_file,'_/library.db')

        os.remove(backup_file)

        return path

    def can_restore(self):

        backup_file = self.cache.get('_/library.db')

        if backup_file:
            return True
        else:
            return False

    def restore(self, backup_file=None):
        '''Restore the database from the remote'''

        if not backup_file:
            # This requires that the cache have and upstream that is also the remote
            backup_file = self.cache.get('_/library.db')

        self.database.restore(backup_file)

        # HACK, fix the dataset root
        try:
            self.database._clean_config_root()
        except:
            print "ERROR for path: {}, {}".format(self.database.dbname, self.database.dsn)
            raise

        os.remove(backup_file)

        return backup_file

    def remote_rebuild(self):
        '''Rebuild the library from the contents of the remote'''

        self.logger.info("Rebuild library from: {}".format(self.upstream))

        #self.database.drop()
        #self.database.create()

        # This should almost always be an object store, like S3. Well, it better be,
        # inst that is the only cache that has the include_partitions parameter.
        rlu = self.upstream.last_upstream()

        remote_partitions = rlu.list(include_partitions=True)

        for rel_path in self.upstream.list():


            path = self.load(rel_path)

            if not path or not os.path.exists(path):
                self.logger.error("ERROR: Failed to get load for relpath: '{}' ( '{}' )".format(rel_path, path))
                continue

            bundle = DbBundle(path)
            identity = bundle.identity

            self.database.add_file(path, self.cache.repo_id, identity.vid,  'pulled')
            self.logger.info('Installing: {} '.format(bundle.identity.name))
            try:
                self.database.install_bundle_file(identity, path)
            except Exception as e:
                self.logger.error("Failed: {}".format(e))
                continue

            for p in bundle.partitions:

                # This is the slow way to do it:
                # if self.remote.last_upstream().has(p.identity.cache_key):
                if p.identity.cache_key in remote_partitions:
                    self.database.add_remote_file(p.identity)
                    self.logger.info('            {} '.format(p.identity.name))
                else:
                    self.logger.info('            {} Ignored; not in remote'.format(p.identity.name))

    def rebuild(self):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''

        from databundles.bundle import DbBundle

        self.logger.info("Clean database {}".format(self.database.dsn))
        self.database.clean()
        self.logger.info("Create database {}".format(self.database.dsn))
        self.database.create()

        bundles = []

        self.logger.info("Rebuilding from dir {}".format(self.cache.cache_dir))

        for r,d,f in os.walk(self.cache.cache_dir): #@UnusedVariable

            if '/meta/' in r:
                continue

            for file_ in f:

                if file_.endswith(".db"):
                    path_ = os.path.join(r,file_)
                    try:
                        b = DbBundle(path_)
                        # This is a fragile hack -- there should be a flag in the database
                        # that diferentiates a partition from a bundle.
                        f = os.path.splitext(file_)[0]

                        if b.db_config.get_value('info','type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.vname, file_))
                            bundles.append(b)

                    except Exception as e:
                        pass
                        #self.logger.error('Failed to process {}, {} : {} '.format(file_, path_, e))


        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.vname))

            try:
                self.database.install_bundle(bundle)
            except Exception as e:
                self.logger.error('Failed to install bundle {}'.format(bundle.identity.vname))
                continue

            self.database.add_file(bundle.database.path, self.cache.repo_id, bundle.identity.vid,  'rebuilt', type_='bundle')

            for p in bundle.partitions:
                if self.cache.has(p.identity.cache_key, use_upstream=False):
                    self.logger.info('            {} '.format(p.identity.vname))
                    self.database.add_file(p.database.path, self.cache.repo_id, p.identity.vid,  'rebuilt', type_='partition')


        self.database.commit()
        return bundles

    @property
    def info(self):
        return """
------ Library {name} ------
Database: {database}
Cache:    {cache}
Remote:   {remote}
        """.format(name=self.name, database=self.database.dsn,
                   cache=self.cache, remote=self.upstream if self.upstream else '')

    @property
    def dict(self):
        return dict(name=str(self.name),
                    database=str(self.database.dsn),
                    cache=str(self.cache),
                    remote=str(self.upstream) if self.upstream else None)

