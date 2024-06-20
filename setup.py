import re
from setuptools import setup


for line in open('sepj/config/__init__.py'):
    match = re.match("__version__ *= *'(.*)'", line)
    if match:
        __version__, = match.groups()


setup(name='sepj-platform',
      version=__version__,
      description='SEPJ platform to create data pipelines',
      maintainer='Gabriel Pereira',
      maintainer_email='carvalho.gabrielp@gmail.com',
      url='https://github.com/gc-pereira/data-platform',
      packages=['sepj'],
      keywords=['pyspark', 'awswrangler', 'boto3'],
      install_requires=[
          'awswrangler>=0.23.0',
          'scipy>=1.5.1',
          'pandas>=1.1.4',
          'numpy>=1.18.1'
      ],
      license='MIT License'
)