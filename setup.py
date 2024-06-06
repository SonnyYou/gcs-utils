## setup.py
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gcs-utils-py",
    version="0.0.1",
    author="Sonny You",
    author_email="wunjheng.you@gmail.com",
    description="A GCS utils package for Python including GCS client and GCS file utils.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SonnyYou/gcs-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
    ],
    license="MIT",
    python_requires=">=3.10",
)
