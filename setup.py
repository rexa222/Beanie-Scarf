from setuptools import setup, find_packages


def read_version():
    version = {}
    with open('scarf/version.py', 'r') as f:
        exec(f.read(), version)
        return version['__version__']


setup(
    name='beanie-scarf',
    version=read_version(),
    description='A set of complement useful tools added to Beanie (MongoDB ODM)',
    author='alex',
    author_email='rexa222@outlook.com',
    packages=find_packages(include=['scarf', 'scarf.*']),  # would be the same as name
    install_requires=open('requirements.txt').readlines(),  # external packages acting as dependencies
)
