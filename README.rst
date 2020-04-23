Add Server Message Block (SMB) storage support to dtool
=======================================================

.. image:: https://badge.fury.io/py/dtool-smb.svg
   :target: http://badge.fury.io/py/dtool-smb
   :alt: PyPi package

- GitHub: https://github.com/IMTEK-Simulation/dtool-smb
- PyPI: https://pypi.python.org/pypi/dtool-smb
- Free software: MIT License

Features
--------

- Copy datasets to and from SMB storage
- List all the datasets in an SMB share
- Create datasets directly to an SMB share


Installation
------------

To install the dtool-smb package::

    pip install dtool-smb


Configuration
-------------

Then create the file ``.config/dtool/dtool.json`` and add the SMB account using the format below::

    ```
    {
      "DTOOL_SMB_USERNAME_jicinformatics": "<username>",
      "DTOOL_SMB_PASSWORD_jicinformatics": "<password>",
      "DTOOL_SMB_SERVER_NAME_jicinformatics": "<server-name>",
      "DTOOL_SMB_SERVER_PORT_jicinformatics": <server-port>,
      "DTOOL_SMB_DOMAIN_jicinformatics": "<smb-domain>",
      "DTOOL_SMB_SERVICE_NAME_jicinformatics": "<smb-share>",
      "DTOOL_SMB_PATH_jicinformatics": "<path-within-share>"
    }
    ```

Note that <server-port> is typically 445. The name of the 'share' is specified through the
``DTOOL_SMB_SERVICE_NAME_*`` key.

Usage
-----

To copy a dataset from local disk (``my-dataset``) to an SMB storage account
(``jicinformatics``) one can use the command below::

    dtool cp ./my-dataset smb://jicinformatics/

To list all the datasets in an SMB share one can use the command below::

    dtool ls smb://jicinformatics/

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.

Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-http <https://github.com/jic-dtool/dtool-http>`_
- `dtool-s3 <https://github.com/jic-dtool/dtool-s3>`_
- `dtool-irods <https://github.com/jic-dtool/dtool-irods>`_
- `dtool-ecs <https://github.com/jic-dtool/dtool-ecs>`_
- `dtool-azure <https://github.com/jic-dtool/dtool-azure>`_
