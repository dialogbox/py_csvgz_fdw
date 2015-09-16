import subprocess
from setuptools import setup, find_packages, Extension

setup(
  name='csvgz_fdw',
  version='0.1.0',
  author='Jason Kim <jason.kim@enterprisedb.com>',
  license='Postgresql',
  packages=['csvgz_fdw']
)
