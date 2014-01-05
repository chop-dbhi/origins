from distutils.core import setup


kwargs = {
    'packages': ['origins'],
    'test_suite': 'test_suite',
    'name': 'origins',
    'version': __import__('origins').get_version(),
    'install_requires': ['graphlib>=0.9.1'],
    'author': 'Byron Ruth',
    'author_email': 'b@devel.io',
    'description': 'Data introspection, indexer, and semantic analyzer',
    'license': 'BSD',
    'keywords': 'data element indexer crawler introspection semantic',
    'url': 'https://github.com/cbmi/origins/',
    'classifiers': [
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
    ],
}

setup(**kwargs)
