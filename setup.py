from setuptools import setup, find_packages

setup(
    name="custom_logging",
    version="0.1.0",
    packages=find_packages(),
    description="Football analysis package for Databricks",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=[
        "pyyaml>=6.0,<7.0",
        "pandas>=2.0.0,<2.3.0",  # Match your environment
        "numpy>=1.21.0,<2.0.0"   # Avoid major version jumps
    ],
    python_requires=">=3.7",
    package_data={"custom_logging": ["config/*.yaml"]},  # Include YAML files
)