'''
    setup.py
    Setup file to make installing holoclean easier
'''

from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
    name='holoclean',
    version='0.1.0',
    description='Holoclean is a statistical inference engine to impute, clean, and enrich data.',
    author='HoloClean',
    author_email='contact@holoclean.io',
    license='Apache License 2.0',
    url='http://www.holoclean.io/',
    packages=['holoclean'],
    install_requires=[
        'ipython==5.6.0',
        'jupyter==1.0.0',
        'click==6.7 ',
        'Distance==0.1.3',
        'future==0.16.0',
        'futures==3.2.0',
        'psycopg2==2.7.4',
        'py4j==0.10.6',
        'pyspark==2.3.0',
        'torch==0.3.1',
        'torchvision==0.2.0',
        'tqdm==4.20.0',
        'scipy==1.0.1',
        'Pandas==0.20.3'
    ]
)

