from setuptools import setup, find_packages

# 读取依赖
with open('requirements.txt') as f:
    required = f.read().splitlines()

# 过滤掉注释和空行
required = [r.strip() for r in required if r.strip() and not r.startswith('#')]

setup(
    name="likob",
    version="0.1.0",
    packages=find_packages(),
    install_requires=required,
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'black>=22.0.0',
            'mypy>=1.0.0',
            'pylint>=2.17.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'likob=likob.cli:main',
        ],
    },
    author="lik639259",
    author_email="3605898158@qq.com",
    description="A simple SQL database implementation",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/lik639259/likOb",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)