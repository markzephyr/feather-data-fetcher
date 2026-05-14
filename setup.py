from setuptools import setup, find_packages

setup(
    name="feather-data-fetcher",
    version="0.1.0",
    description="Production-grade data ingestion engine for Quantitative Finance and AI.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Feather AI",
    author_email="hello@featherai.com",
    url="https://github.com/featherai/feather-data-fetcher",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.1",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "ccxt>=4.0.0",
        "yfinance>=0.2.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
    python_requires=">=3.8",
)
