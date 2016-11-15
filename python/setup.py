import uuid
from setuptools import setup, find_packages
from pip.req import parse_requirements

__version__ = '0.1.0'
__author__ = 'ShuTong'

install_reqs = parse_requirements('requirements.txt', session=uuid.uuid1)
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='HDFSTools',
    version=__version__,
    author=__author__,
    packages=find_packages(),
    install_requires=reqs
)
