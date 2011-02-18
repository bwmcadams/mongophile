#!/usr/bin/env python
#
# Copyright (c) 2010 Brendan W. McAdams <brendan@10gen.com> 
#

try:
    from setuptools import setup, find_packages
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='MongoPhile',
    version='0.1.0',
    author='Brendan W. McAdams',
    author_email='brendan@10gen.com',
    packages=['mongophile'],
    url='http://github.com/bwmcadams/mongophile',
    description='Parse utilities for the MongoDB Profiler log',
    entry_points="""
    [console_scripts]
    parse_mongo_profile = mongophile.parser:main
    """
)
