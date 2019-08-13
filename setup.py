# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Install datacommons."""
from setuptools import setup

# Package metadata.
NAME = 'datacommons'
DESCRIPTION = 'A library to access Data Commons API.'
URL = 'https://github.com/google/datacommons'
EMAIL = 'datacommons@google.com'
AUTHOR = 'datacommons.org'
REQUIRES_PYTHON = '>=2.7.0'
VERSION = '0.4.3'

REQUIRED = [
    'google-api-python-client',
    'httplib2',
    'numpy',
    'oauth2client',
    'pandas',
]

PACKAGES = ['datacommons']
PACKAGE_DIR = {'datacommons': 'datacommons'}

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=EMAIL,
    maintainer=AUTHOR,
    maintainer_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=PACKAGES,
    package_dir=PACKAGE_DIR,
    install_requires=REQUIRED,
    include_package_data=True,
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development',
    ],
)
