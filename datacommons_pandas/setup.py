# Copyright 2020 Google Inc.
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
"""Build and distribute the datacommons_pandas package to PyPI."""
from setuptools import setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

# Package metadata.
NAME = 'datacommons_pandas'
DESCRIPTION = 'A library to create pandas objects using the Data Commons Python API.'
URL = 'https://github.com/datacommonsorg/api-python'
EMAIL = 'support@datacommons.org'
AUTHOR = 'datacommons.org'
REQUIRES_PYTHON = '>=3.7'
VERSION = '0.0.3'
REQUIRED = ['pandas', 'six', 'requests']
PACKAGES = ['datacommons_pandas']

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    maintainer=AUTHOR,
    maintainer_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=PACKAGES,
    install_requires=REQUIRED,
    include_package_data=True,
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development',
    ],
)
