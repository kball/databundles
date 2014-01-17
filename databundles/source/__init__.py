
"""

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt



import os


def load_bundle(bundle_dir):
    from databundles.run import import_file

    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)

    return mod.Bundle

class SourceTree(object):

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def list(self, datasets=None, key='vid'):
        from ..identity import LocationRef, Identity

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, _, files in os.walk(self.base_dir):
            if 'bundle.yaml' in files:

                bundle_class = load_bundle(root)
                bundle = bundle_class(root)

                ident = bundle.identity
                ck = getattr(ident, key)
                ident.locations.set(LocationRef.LOCATION.SOURCE)
                datasets[ck] = ident


        return sorted(datasets.values(), key=lambda x: x.vname)

