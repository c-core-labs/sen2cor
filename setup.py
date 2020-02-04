'''
Created on 22.04.2015
@author: TPZV
usage python setup.py build_ext
'''
import platform
from setuptools import setup
from Cython.Build import cythonize

system = platform.system()
if system == 'Windows':
	name = 'L2A_AtmCorr.pyd'
else:
	name = 'L2A_AtmCorr.so'

setup(
	name = name,
    ext_modules = cythonize('L2A_AtmCorr.py'),
)

if __name__ == '__main__':
    pass
