"""Geo build  bundles load shapefiles into sqlite databases.

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__author__ = 'eric'

from bundle import BuildBundle

class GeoBuildBundle(BuildBundle):


    def __init__(self, bundle_dir=None):
        '''
        '''

        super(GeoBuildBundle, self).__init__()


    def meta(self):
        from databundles.geo.sfschema import copy_schema

        self.database.create()

        self.schema.clean()

        for table, url in self.config.build.sources.items():
            with self.session:
                copy_schema(self.schema, table_name=table, path=url)

        self.schema.write_schema()

        return True


    def build(self):
        for table, url in self.config.build.sources.items():
            p = self.partitions.new_geo_partition(table=table, shape_file=url)

        return True