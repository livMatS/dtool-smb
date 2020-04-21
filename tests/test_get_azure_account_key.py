"""Test the logic of getting a storage account key."""

import os
import json

from . import tmp_dir_fixture  # NOQA


def test_get_azure_account_key(tmp_dir_fixture):  # NOQA
    from dtool_azure.utils import get_azure_account_key

    config = {"DTOOL_AZURE_ACCOUNT_KEY_accountname": "account_key"}
    config_path = os.path.join(tmp_dir_fixture, "my.conf")
    with open(config_path, "w") as fh:
        json.dump(config, fh)

    account_key = get_azure_account_key("accountname", config_path)
    assert account_key == "account_key"
