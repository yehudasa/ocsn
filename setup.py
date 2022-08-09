#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='ocsn',
    version='0.0.1',
    packages=find_packages(),

    author='Yehuda Sadeh',
    author_email='yehuda@redhat.com',
    description='Overlay Cloud Storage Network Manager',
    license='MIT',
    keywords='ocsn',

    install_requires=[
        'redis',
        ],

    entry_points={
        'console_scripts': [
            'ocsn = cli:main',
            ],
        },

    )
