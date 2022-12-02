#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask_deltalake",
    version="0.1",
    description="Dask + Deltalake ",
    maintainer="hayesgb",
    maintainer_email="hayesgb@gmail.com",
    license="MIT",
    packages=["dask_deltalake"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"dev": ["pytest", "requests", "pytest-cov>=2.10.1"]},
    package_data={"dask_deltalake": ["*.pyi" "__init__.pyi", "core.pyi"]},
    include_package_data=True,
    zip_safe=False,
)
