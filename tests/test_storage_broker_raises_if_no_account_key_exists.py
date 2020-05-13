"""Test that the AzureStorageBroker raises if the account is not defined."""

import pytest


def test_raises_if_account_is_not_defined():

    from dtool_smb.storagebroker import SMBStorageBroker

    with pytest.raises(KeyError):
        SMBStorageBroker("smb://doesnt_exist/fake_UUID")
