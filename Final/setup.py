from setuptools import find_packages, setup

setup(
    name='podcastpy',
    packages=find_packages(),
    scripts=["podcastpy.config.py"],
    version='0.1.0',
    description='Package for podcasts data processing',
    author='Rafal B',
    license='',
)
