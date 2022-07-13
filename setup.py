#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                setup.py
# License:             BSD-3-Clause
# Author:              sr
# Date:                13.07.2022
# Last Modified Date:  13.07.2022
# Last Modified By:    Simeon Reusch (simeon.reusch@desy.de)

from setuptools import find_namespace_packages, setup

setup(
    name="ampel-apitools",
    version="0.8.3",
    packages=find_namespace_packages(),
    package_data={
        "": ["*.json", "py.typed"],  # include any package containing *.json files
        "conf": [
            "*.json",
            "**/*.json",
            "**/**/*.json",
            "*.yaml",
            "**/*.yaml",
            "**/**/*.yaml",
            "*.yml",
            "**/*.yml",
            "**/**/*.yml",
        ],
    },
    install_requires=[
        "ampel-ztf>=0.8.0a4,<0.9",
        "astropy",
        "appdirs",
        "requests",
        "backoff",
        "numpy",
        "pandas",
    ],
)
