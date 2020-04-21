from setuptools import setup

url = "https://github.com/IMTEK-Simulation/dtool-smb"
version = "0.1.0"
readme = open('README.rst').read()

setup(
    name="dtool-smb",
    packages=["dtool_smb"],
    version=version,
    description="Add SMB (Server Message Block) dataset support to dtool",
    long_description=readme,
    include_package_data=True,
    author="Lars Pastewka",
    author_email="lars.pastewka@imtek.uni-freiburg.de",
    url=url,
    install_requires=[
        "dtoolcore>=3.17",
        "pysmb"
    ],
    entry_points={
        "dtool.storage_brokers": [
            "SMBStorageBroker=dtool_smb.storagebroker:SMBStorageBroker",
        ],
    },
    download_url="{}/tarball/{}".format(url, version),
    license="MIT"
)
