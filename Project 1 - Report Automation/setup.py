# run pip3 install -e .
# https://godatadriven.com/blog/a-practical-guide-to-using-setup-py/
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(name='report',
      version='0.1.1',
      packages=['report','report.src', 'report.src.operators'],
      install_requires=
      required
    #   [
    #     'numpy',
    #     'pandas',
    #     'openpyxl',
    #     'discord==1.7.3'
    # ]
    )