from setuptools import setup, find_packages

setup(
    name='datacustodia',
    version="0.0.2",
    description='Pacote python para utilização na plataforma de dados',
    maintainer='Gabriel Pereira',
    maintainer_email='carvalho.gabrielp@gmail.com',
    url='https://github.com/gc-pereira/data-platform',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    keywords=['pyspark', 'awswrangler', 'boto3'],
    install_requires=[
        'awswrangler==3.9.1',
        'pydantic',
        'pyspark>=3.0.1',
    ],
    license='MIT License'
)