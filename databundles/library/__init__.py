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

    remote = new_cache(config['remote']) if 'remote' in config else None


    config['name'] = config['_name'] if '_name' in config else 'NONE'

    for key in ['_name', 'filesystem', 'database', 'remote' ]:
        if key in config:
            del config[key]


    if remote and (not isinstance(remote, RemoteMarker)
                   and not isinstance(remote.last_upstream(), RemoteMarker)):
        raise ConfigurationError("Library remote must hace a RemoteMarker interface: {}".format(config))

    # Idea for integrating the remote into the cache.
    #lus = cache.last_upstream()
    #lus.upstream = remote
    #remote = remote.last_upstream

    l =  Library(cache = cache,
                 database = database,
                 remote = remote,
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

    # Return value for get()
    Return = collections.namedtuple('Return',['bundle','partition'])

    # Return value for earches
    ReturnDs = collections.namedtuple('ReturnDs',['dataset','partition'])

    def __init__(self, cache,database, name =None, remote=None, sync=False, require_upload = False, host=None,port = None):
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
        self._remote = remote
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

        return self.__class__(self.cache, self.database.clone(), self._remote, self.sync, self.require_upload, self.host, self.port)


    def list(self, with_meta = True, add_remote=True):
        '''Lists all of the datasets in the partition, optionally with
        metadata. Does not include partitions. This returns a dictionary
        in  a form that is similar to the remote and source lists. '''
        import socket

        datasets = {}

        if add_remote and self.remote:
            try:
                for k,v in self.remote.list(with_metadata=with_meta).items():

                    if v and v['identity']['id'] != 'a0':
                        v['identity']['location'] = ['R',' ']
                        v['identity']['remote_version'] = int(v['identity']['revision']) # b/c 'revision' can be overwriten by library entry
                        v['identity']['library_version'] = 0
                        datasets[k] =  v['identity']
            except socket.error:
                pass


        self.database.list(datasets=datasets)

        return sorted(datasets.values(), key=lambda x: x['vname'])


    @property
    def remote(self):
        if self._remote:
            return self._remote # When it is a URL to a REST interface.
        else:
            return None

    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return self._database

    def load(self, rel_path):
        '''Load a record into the cache from the remote'''
        from ..util.flo import copy_file_or_flo

        if not self.remote.has(rel_path):
            return None

        source = self.remote.get_stream(rel_path, return_meta=True)

        sink = self.cache.put_stream(rel_path, metadata=source.meta)

        try:
            copy_file_or_flo(source, sink)
        except:
            self.cache.remove(rel_path, propagate=True)
            raise

        source.close()
        sink.close()

        return self.cache.path(rel_path)


    def config(self, bp_id):

        from ..cache import RemoteMarker

        d,p = self.get_ref(bp_id)

        try:
            api = self.remote.get_upstream(RemoteMarker)
        except AttributeError: # No api
            api = self.remote


        if self.cache.has(d.cache_key):
            b = self.get(d.vid)
            config = b.db_config.dict

        elif api:
            from client.exceptions import NotFound

            try:
                r = api.get(d.vid, p.vid if p else None)
                if r:
                    remote_d = r['dataset']['config']

            except NotFound as e:
                pass
        else:
            return None

    def _get_remote_dataset(self, dataset, cb=None):
        from ..identity import new_identity
        from ..util import copy_file_or_flo


        try:# ORM Objects
            identity = new_identity(dataset.to_dict())
        except:# Tuples
            identity = new_identity(dataset._asdict())

        source = self.remote.get_stream(identity.cache_key)

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

        from identity import  new_identity
        from util import copy_file_or_flo

        identity = new_identity(partition.to_dict(), bundle=bundle)

        p = bundle.partitions.get(identity.id_) # Get partition information from bundle

        if not p:
            from databundles.dbexceptions import NotFoundError
            raise NotFoundError("Failed to find partition {} in bundle {}"
                                .format(identity.name, bundle.identity.name))

        if os.path.exists(p.database.path) and not p.database.is_empty():

            from databundles.dbexceptions import ConflictError
            raise ConflictError("Trying to get {}, but file {} already exists".format(identity.vname, p.database.path))

        # Now actually get it from the remote.

        source = self.remote.get_stream(p.identity.cache_key, return_meta=True)

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


    def get(self,bp_id, force = False, cb=None):
        '''Get a bundle, given an id string or a name '''

        # Get a reference to the dataset, partition and relative path
        # from the local database.

        ip, dataset = self.database.resolver.resolve_ref_one(bp_id)

        if dataset and dataset.partition:
            return self._get_partition(dataset, dataset.partition, force, cb=cb)
        elif dataset:
            return self._get_dataset(dataset, force, cb=cb)
        else:
            return False


    def _get_dataset(self, dataset, force = False, cb=None):

        from sqlite3 import  DatabaseError

        # Try to get the file from the cache.
        abs_path = self.cache.get(dataset.cache_key, cb=cb)

        # Not in the cache, try to get it from the remote library,
        # if a remote was set.

        if ( not abs_path or force )  and self.remote :
            abs_path = self._get_remote_dataset(dataset)

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

            if self.remote:
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
                raise NotFoundError(("Didn't find partition {} in bundle {}. Partition "+
                                    "found in bundle, but path {} ({}?) not in local "+
                                    "library and remote not set. force={}, is_empty={}").format(
                            p.identity.name, r.identity.name,p.database.path, rp, force, p.database.is_empty()))


            # Ensure the file is in the local library.

        ds, pt= self.database.get_id(p.identity.vid)

        if not pt:
            self.database.add_file(p.database.path, self.cache.repo_id, p.identity.vid, 'pulled')
            self.database.install_partition(p.bundle, p.identity)

        p.library = self

        # Unsetting the database is particularly necessary for Hdf partitions where the file will get loaded when the partition
        # is first created, which creates and empty file, and the empty file is carried forward after the file on
        # disk is changed.
        p.unset_database()
        r.partition = p

        return r


    def find(self, query_command):

        return self.database.find(query_command)


    def remote_find(self, query_command):
        from ..cache import RemoteMarker
        import socket

        try:
            api = self.remote.get_upstream(RemoteMarker).api
        except AttributeError: # No api
            try:
                api = self.remote.api
            except AttributeError: # No api
                return False

        try:
            r = api.find(query_command)
        except socket.error:
            self.logger.error("Connection to remote failed")
            return False

        if not r:
            return False

        return r

    def path(self, rel_path):
        """Return the cache path for a cache key"""

        return self.cache.path(rel_path)


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

    def put_remote_ref(self,identity):
        '''Store a reference to a partition that has been uploaded directly to the remote'''
        pass

    def put_file(self, identity, file_path, state='new', force=False):
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

        if self.remote and self.sync:
            self.remote.put(identity, file_path)



        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ ='bundle')
        else:
            self.database.add_file(dst, self.cache.repo_id, identity.vid,  state, type_ = 'partition')

        return dst, identity.cache_key, self.cache.last_upstream().path(identity.cache_key)

    def put(self, bundle, force=False):
        '''Install a bundle or partition file into the library.

        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object

        '''
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        if not isinstance(bundle, (PartitionInterface, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")


        bundle.identity.name # throw exception if not right type.

        dst, cache_key, url = self.put_file(bundle.identity, bundle.database.path, force=force)

        return dst, cache_key, url

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.cache.remove(bundle.identity.cache_key, propagate = True)

    def clean(self, add_config_root=True):
        self.database.clean(add_config_root=add_config_root)

    def purge(self):
        """Remove all records from the library database, then delete all
        files from the cache"""
        self.clean()
        self.cache.clean()



    @property
    def datasets(self):
        '''Return an array of all of the dataset records in the library database'''
        from databundles.orm import Dataset

        return [d for d in self.database.session.query(Dataset).all() if d.vid != ROOT_CONFIG_NAME_V]

    @property
    def partitions(self):
        '''Return an array of all of the dataset records in the library database'''
        from databundles.orm import Partition, Dataset

        return [r for r in self.database.session.query(Dataset, Partition).join(Partition).all()
               if r.Dataset.vid != ROOT_CONFIG_NAME_V]

    @property
    def new_files(self):
        '''Generator that returns files that should be pushed to the remote
        library'''

        new_files = self.database.get_file_by_state('new')

        for nf in new_files:
            yield nf

    def push(self, file_=None):
        """Push any files marked 'new' to the remote

        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files.

        """

        if not self.remote:
            raise Exception("Can't push() without defining a remote. ")

        if file_ is not None:

            dataset, partition = self.database.get_id(file_.ref)

            if not dataset:
                raise Exception("Didn't get id from database for file ref: {}, type {}".format(file_.ref, file_.type_))

            if partition:
                identity = partition
            else:
                identity = dataset

            self.remote.put(file_.path, identity.cache_key, metadata=identity.to_meta(file=file_.path))
            file_.state = 'pushed'

            self.database.commit()
        else:
            for file_ in self.new_files:
                self.push(file_)

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

        path = self.remote.put(backup_file,'_/library.db')

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

        self.logger.info("Rebuild library from: {}".format(self.remote))

        #self.database.drop()
        #self.database.create()

        # This should almost always be an object store, like S3. Well, it better be,
        # inst that is the only cache that has the include_partitions parameter.
        rlu = self.remote.last_upstream()

        remote_partitions = rlu.list(include_partitions=True)

        for rel_path in self.remote.list():


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
                   cache=self.cache, remote=self.remote if self.remote else '')

