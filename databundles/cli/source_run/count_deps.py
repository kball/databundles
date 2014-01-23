'''Script for source run to print the umber of dependencies that a source bundle has'''

def run(bundle_dir, bundle, repo):

    deps = bundle.config.build.get('dependencies',{})

    if len(deps) == 0:
        print len(deps), bundle_dir
