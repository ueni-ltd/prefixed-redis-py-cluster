from setuptools import setup

setup(
    name='prefixed-redis-py-cluster',
    version='0.0.1',
    packages=['prefixed_redis_cluster'],
    url='',
    license='UENI Internal',
    author='michal',
    author_email='michal@ueni.com',
    description='',
    install_requires=[
        'redis-py-cluster==1.3.4'
    ]
)
