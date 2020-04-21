Add Azure storage support to dtool
==================================

.. image:: https://badge.fury.io/py/dtool-azure.svg
   :target: http://badge.fury.io/py/dtool-azure
   :alt: PyPi package

- GitHub: https://github.com/jic-dtool/dtool-azure
- PyPI: https://pypi.python.org/pypi/dtool-azure
- Free software: MIT License

Features
--------

- Copy datasets to and from Azure storage
- List all the datasets in a Azure storage account
- Create datasets directly in Azure storage


Installation
------------

To install the dtool-azure package::

    pip install dtool-azure


Configuration
-------------

Install the Azure command line client via::

    pip install azure-cli

(you may wish to install this in a virtual environment)

Then::

    az login

To log into Azure.

Then you need to run (changing the resource name/group as appropriate)::

    az storage account show-connection-string --name jicinformatics --resource-group jic_informatics_resources_ukwest

Then create the file ``.config/dtool/dtool.json`` and add the Azure account name and key using the format below::

    {
        "DTOOL_AZURE_ACCOUNT_KEY_<ACCOUNT NAME>": "<KEY HERE>"
    }

Changing the account name and key as appropriate. For example if the account
name was "jicinformatics" and the key "some-secret-token"::

    {
        "DTOOL_AZURE_ACCOUNT_KEY_jicinformatics": "some-secret-token"
    }


Usage
-----

To copy a dataset from local disk (``my-dataset``) to an Azure storage account
(``jicinformatics``) one can use the command below::

    dtool copy ./my-dataset azure://jicinformatics/

To list all the datasets in an Azure storage account one can use the command below::

    dtool ls azure://jicinformatics/

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.

Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-http <https://github.com/jic-dtool/dtool-http>`_
- `dtool-s3 <https://github.com/jic-dtool/dtool-s3>`_
- `dtool-irods <https://github.com/jic-dtool/dtool-irods>`_
- `dtool-ecs <https://github.com/jic-dtool/dtool-ecs>`_
