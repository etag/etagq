#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='etagq',
      version='0.0',
      packages= find_packages(),
      install_requires=[
          'sqlalchemy==1.2.18',
          'psycopg2==2.7.7',
          'pandas>=0.24.1',
          'pytz',
      ],
      include_package_data=True,
)
