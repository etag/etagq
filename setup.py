#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='etagq',
      version='0.0',
      packages= find_packages(),
      install_requires=[
          'sqlalchemy',
          'psycopg2',
          'pandas',
      ],
      include_package_data=True,
)
