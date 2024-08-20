from mypyc.build import mypycify
from setuptools import setup


setup(
    name='sidecar',
    version='0.0.1',
    packages=['.'],
    ext_modules=mypycify(
        ['--disallow-untyped-defs',
            'sidecar.py'
        ],
        opt_level='fast',
        separate=False
    ),
)
