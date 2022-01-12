import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="satem_data",
    version="0.0.1",
    author="CREA Dev Team",
    author_email="hubert@energyandcleanair.org",
    description="A package to retrieve emission and feature data from SatEm related databases.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/energyandcleanair/satem_data",
    project_urls={
        "Bug Tracker": "https://github.com/energyandcleanair/satem_data/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "satem_data"},
    packages=setuptools.find_packages(where="satem_data"),
    python_requires=">=3.6",
)