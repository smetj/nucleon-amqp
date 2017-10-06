import sys
from setuptools import setup, find_packages

import os.path

thisdir = os.path.dirname(__file__)
generated_files = [
    os.path.join(thisdir, 'nucleon', 'amqp', 'spec.py'),
    os.path.join(thisdir, 'nucleon', 'amqp', 'spec_exceptions.py')
]
if any(not os.path.exists(f) for f in generated_files):
    sys.exit('Please run `make` first.')


setup(
    name='nucleon.amqp',
    version='0.2.1',
    description='A gevent-based AMQP library with fast, synchronous API',
    author='Daniel Pope',
    author_email='ext-dan.pope@vertu.com',
    url='https://bitbucket.org/lordmauve/nucleon.amqp',
    packages=find_packages(),
    namespace_packages=['nucleon'],
    install_requires=[
        'gevent==1.0b4',
    ],
    dependency_links=[
        'http://code.google.com/p/gevent/downloads/list',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
