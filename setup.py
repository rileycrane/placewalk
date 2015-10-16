import sys

if sys.version_info < (2, 7):
    print(sys.stderr, "{}: need Python 2.7 or later.".format(sys.argv[0]))
    print(sys.stderror, "Your lame Python is {}".format(sys.version))
    sys.exit(1)

from setuptools import setup

setup(
    name='placewalk',
    version='0.1.0',
    description='.',
    url='https://github.com/rileycrane/placewalk')


import os

from setuptools import find_packages
from setuptools import setup


cur_dir = os.path.dirname(__file__)
readme = os.path.join(cur_dir, 'README.md')
if os.path.exists(readme):
    with open(readme) as fh:
        long_description = fh.read()
else:
    long_description = ''

setup(
    name='placewalk',
    version=__import__('placewalk').__version__,
    description='placewalk',
    long_description=long_description,
    author='Riley Crane',
    author_email='rileycrane@gmail.com',
    url='http://github.com/rileycrane/placewalk/',
    install_requires=['redis'],
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='placewalk.tests',
)