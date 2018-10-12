from setuptools import setup, find_packages

setup(
    name='ShapeShifter',
    url='https://github.com/srp33/ShapeShifter',
    author='Brandon Fry',
    # author_email='',
    # Needed to actually package something
    packages=find_packages(['shapeshifter']),
    # Needed for dependencies
    install_requires=['pandas', 'pyarrow', 'sqlalchemy'],
    version='0.1',
    license='MIT',
    description='A tool for managing large datasets',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)