#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from setuptools import setup, find_packages

setup(
    name='pyservicelib',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'requests',
    ],
)