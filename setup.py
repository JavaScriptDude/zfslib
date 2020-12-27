# Deploy to PyPi
# % python3 setup.py sdist bdist_wheel
# % python3 -m twine upload --skip-existing dist/*
from setuptools import setup
import os

VERSION = "0.4"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()

CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.7',
    'Topic :: System :: Filesystems',
    'Topic :: Utilities',
]

setup(
    name="zfslib",
    description="ZFS Python Library",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Timothy C. Quinn",
    url="https://github.com/JavaScriptDude/zfslib",
    project_urls={
        "Issues": "https://github.com/JavaScriptDude/zfslib/issues",
        "CI": "https://github.com/JavaScriptDude/zfslib/actions",
        "Changelog": "https://github.com/JavaScriptDude/zfslib/releases",
    },
    license="GNU General Public License v3 (GPLv3)",
    version=VERSION,
    packages=["zfslib"],
    install_requires=[],
    extras_require={"test": ["pytest"]},
    tests_require=["zfslib[test]"],
    python_requires=">=3.7",
    classifiers=CLASSIFIERS,
)
