"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbmanage import prt


def ckan_command(args,rc, src):
    from databundles.dbexceptions import ConfigurationError
    import databundles.client.ckan
    import requests
    
    repo_name = args.name
    
    repo_group = rc.group('repository')
    if not repo_group.get(repo_name): 
        raise ConfigurationError("'repository' group in configure either nonexistent"+
                                 " or missing {} sub-group ".format(repo_name))
    
    repo_config = repo_group.get(repo_name)
    
    api = databundles.client.ckan.Ckan( repo_config.url, repo_config.key)   
    
    if args.subcommand == 'package':
        try:
            pkg = api.get_package(args.term)
        except requests.exceptions.HTTPError:
            return
        
        if args.use_json:
            import json
            prt(json.dumps(pkg, sort_keys=True, indent=4, separators=(',', ': ')))
        else:
            import yaml
            yaml.dump(args, indent=4, default_flow_style=False)

    else:
        pass
 
