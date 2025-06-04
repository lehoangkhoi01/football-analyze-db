from setuptools import setup, find_packages

setup(
    name="logging",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",  # For YAML config
    ],
    python_requires=">=3.7",
)