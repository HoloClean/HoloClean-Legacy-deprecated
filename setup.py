"""For pip."""

from setuptools import setup, find_packages

exec(open('holoclean/version.py').read())
setup(
    name='holoclean',
    version=__version__,
    packages=find_packages(),
    install_requires=['futures'],
    package_data={'': ['holocelan/lib/mysql-connector-java-5.1.44-bin.jar']}
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'holoclean = holoclean.holoclean:main',
        ],
    },
)
