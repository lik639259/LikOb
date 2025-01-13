from setuptools import setup, find_packages
import io

with io.open("README.md", encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="likob",
    version="1.0.3",
    packages=find_packages(),
    install_requires=[
        # 
    ],
    entry_points={
        'console_scripts': [
            'likob=likob.cli:main',
        ],
    },
    author="lik639259",
    author_email="3605898158@qq.com",
    description="A lightweight and powerful SQL database implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lik639259/LikOb",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)