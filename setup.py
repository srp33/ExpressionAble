from setuptools import setup, find_packages

setup(
    name='shapeshifter',
    url='https://github.com/srp33/ShapeShifter',
    author='Piccolo Lab',
    author_email='stephen_piccolo@byu.edu',
    packages=find_packages(exclude=['Tests*', 'docs*']),
    install_requires=['pandas', 'pyarrow', 'matplotlib==3.0.3', 'numpy==1.15.4', 'sqlalchemy', 'xlsxwriter', 'tables', 'xlrd', 'nbformat'],
    version=open('VERSION').read().strip(),
    license='MIT',
    description='A tool for managing large datasets',
    long_description=open('README.md').read(),
)

