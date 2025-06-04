from setuptools import setup, find_packages

setup(
    name="football_analyze",
    version="0.1.0",
    packages=find_packages(),
    description="Football analysis package for Databricks",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "pyyaml>=6.0",  # For YAML config
    ],
    python_requires=">=3.7",
    author="Khoi Le",
    author_email="khoileit01@gmail.com",
    url="https://github.com/lehoangkhoi01/football_analyze-db",
)