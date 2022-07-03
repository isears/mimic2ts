import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

exec(open("src/mimic2ts/version.py").read())

setuptools.setup(
    name="mimic2ts",
    version=__version__,
    author="Isaac Sears",
    author_email="isaac.j.sears@gmail.com",
    description="Convert MIMIC IV tabular data into timeseries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/isears/mimic2ts",
    project_urls={
        "Bug Tracker": "https://github.com/isears/mimic2ts/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    install_requires=["dask", "pandas", "numpy"],
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
