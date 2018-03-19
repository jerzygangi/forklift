from distutils.core import setup

setup(
    name='forklift',
    version='0.6.1',
    author='Jerzy J. Gangi',
    author_email='jerzy@jerzygangi.com',
    packages=['forklift'],
    scripts=[],
    url='http://jerzygangi.com',
    description='ETL for Spark and Airflow',
    install_requires=[
        'xlsxwriter',
        ]
)
