# -*- coding: utf-8 -*-
import re
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=False)

REQUIRES = [str(ir.req) for ir in install_reqs]


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


def find_version(fname):
    '''Attempts to find the version number in the file names fname.
    Raises RuntimeError if not found.
    '''
    version = ''
    with open(fname, 'r') as fp:
        reg = re.compile(r'__version__ = [\'"]([^\'"]*)[\'"]')
        for line in fp:
            m = reg.match(line)
            if m:
                version = m.group(1)
                break
    if not version:
        raise RuntimeError('Cannot find version information')
    return version

__version__ = find_version("tymongoimport.py")


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content

setup(
    name='tymongoimport',
    version="0.1.0",
    description='A replacement for mongoimport using the API and supporting only jsonl files',
    long_description=read("README.md"),
    author='Miguel Cabrera',
    author_email='miguel.cabrera@trustyou.net',
    url='https://github.com/mfcabrera/tymongoimport',
    install_requires=REQUIRES,
    license=read("LICENSE"),
    zip_safe=False,
    keywords='tymongoimport',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    py_modules=["tymongoimport"],
    entry_points={
        'console_scripts': [
            "tymongoimport = tymongoimport:main"
        ]
    },
    tests_require=['pytest'],
    cmdclass={'test': PyTest}
)
