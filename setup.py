import setuptools

setuptools.setup(
    name = "o2jobber",
    version = "0.0.1",
    author = "Clemens Hug",
    author_email = "clemens.hug@gmail.com",
    description = "A package for automating job submission to the O2 cluster at HMS",
    packages = setuptools.find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux  ",
    ],
    requires = [
        "pyyaml",
        "scp",
        "paramiko",
        "progressbar2",
    ]
)
