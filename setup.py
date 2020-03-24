#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='etagq',
      version='0.8.4',
      packages= find_packages(),
      package_data={'etagq':['tasks/data/*']},
      install_requires=[
          'sqlalchemy==1.3.3',
          'psycopg2==2.8.3',
          'pandas>=0.24.1',
          'pytz',
      ],
      include_package_data=True,
)
