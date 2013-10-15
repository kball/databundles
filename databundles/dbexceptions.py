"""Common exception objects

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
class BundleError(Exception):
    
    def __init__(self, value):
        self.message = value
    def __str__(self):
        return repr(self.message)

class ProcessError(BundleError):
    '''Error in the configuration files'''

class ConfigurationError(BundleError):
    '''Error in the configuration files'''
    
class ResultCountError(BundleError):
    '''Got too many or too few results'''
    
class FilesystemError(BundleError):
    '''Missing file, etc. '''

class NotFoundError(BundleError):
    '''Failed to find resource'''
    
class DependencyError(Exception):
    """Required bundle dependencies not satisfied"""

class NoLock(BundleError):
    '''Error in the configuration files'''

class Locked(BundleError):
    '''Error in the configuration files'''

class QueryError(Exception):
    """Error while executing a query"""
    

class ConflictError(Exception):
    """COnflict with existing resource"""
    
class FatalError(Exception):
    """A Fatal Bundle Error, generated in testing instead of a system exit. """