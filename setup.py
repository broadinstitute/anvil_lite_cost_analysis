from setuptools import find_packages, setup

setup(
    name="alca",
    version="1.0",
    author="Michael Spector",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas",
        "humanize",
        "azure-storage-blob",
        "matplotlib",
    ],
)
