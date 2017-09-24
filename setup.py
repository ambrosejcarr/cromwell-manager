from setuptools import setup

CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "Natural Language :: English",
    "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Scientific/Engineering :: Bio-Informatics",
]

setup(
    name='cromwell_tools',
    version='0.0.1',
    description='Utilities for interacting with a cromwell server',
    url='https://github.com/ambrosejcarr/cromwell_tools.git',
    author='Ambrose J. Carr',
    author_email='mail@ambrosejcarr.com',
    package_dir={'': 'src'},
    packages=['cromwell_tools', 'cromwell_tools/test'],
    install_requires=[
        'google-cloud',
        'requests>=2.13.0'
    ],
    classifiers=CLASSIFIERS,
    include_package_data=True
)
