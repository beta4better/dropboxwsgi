#!/usr/bin/python
#
# This file is part of dropbox_wsgi.
#
# Copyright (c) Dropbox, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from __future__ import with_statement

import os
import re
import shutil
import sys

from setuptools import setup

PARENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

def get_long_description():
    with open(os.path.join(PARENT_DIR, "README.rst")) as f:
        return f.read()

def get_version():
    VERSIONFILE = os.path.join(PARENT_DIR, 'dropbox_wsgi', '_version.py')
    verstrline = open(VERSIONFILE, "rt").read()
    VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        return mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

PACKAGE_NAME = "dropbox_wsgi"
VERSION = get_version()
LONG_DESCRIPTION = get_long_description()
DESCRIPTION = "WSGI-compatible HTTP interface to Dropbox"
CLASSIFIERS = filter(None, map(str.strip,
"""
Development Status :: 5 - Production/Stable
Environment :: No Input/Output (Daemon)
Intended Audience :: Developers
Intended Audience :: System Administrators
License :: OSI Approved :: MIT License
Operating System :: OS Independent
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 3
Topic :: Internet :: WWW/HTTP
Topic :: System :: Networking
Topic :: Software Development :: Libraries :: Python Modules
""".splitlines()))

KEYWORDS="networking"

extra = {}
if sys.version_info >= (3,):
    extra['use_2to3'] = True
    # convert the test code to Python 3
    # because distribute won't do that for us
    # first copy over the tests
    if 'test' in sys.argv:
        shutil.rmtree("3tests", ignore_errors=True)
        shutil.copytree("tests", "3tests")
        subprocess.call(["2to3", "-w", "--no-diffs", "3tests"])
    TEST_SUITE = '3tests'
else:
    TEST_SUITE = 'tests'

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description=DESCRIPTION,
    keywords=KEYWORDS,
    classifiers=CLASSIFIERS,
    long_description=LONG_DESCRIPTION,
    url="https://github.com/rianhunter/dropbox_wsgi",
    author="Rian Hunter",
    author_email="rian@alum.mit.edu",
    packages=['dropbox_wsgi'],
    entry_points={
        'console_scripts': [
            'dropbox_wsgi = dropbox_wsgi.main:main'
            ]
        },
    test_suite=TEST_SUITE,
    license="MIT License",
    **extra
)

