import sys
from setuptools import setup, find_packages

if sys.version_info < (3, 3, 0):
    raise EnvironmentError('Python version not supported')


kwargs = {
    'name': 'origins',

    'version': __import__('origins').get_version(),

    'description': 'Origins is a free and open source service for building '
                   'information dependency graphs across your data, '
                   'systems, and operations.',

    'url': 'https://github.com/cbmi/origins/',

    'author': 'Byron Ruth',

    'author_email': 'b@devel.io',

    'license': 'BSD',

    'keywords': 'graph dependency provenance information service REST',

    'classifiers': [
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],

    'packages': find_packages(exclude=['tests']),

    'install_requires': [
        'docopt>=0.6',
        'requests>=2.2.0,<2.3',
        'uritemplate>=0.6',
        'flask>=0.10,<0.11',
        'flask-restful>=0.2,<0.3',
        'flask-cors>=1.7,<1.8',
    ],

    'scripts': ['bin/origins'],
}

setup(**kwargs)
