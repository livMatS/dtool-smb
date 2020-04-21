"""Test that the AzureStorageBroker raises if the account is not defined."""

import pytest


def test_raises_if_account_is_not_defined():

    from dtool_azure.storagebroker import AzureStorageBroker

    with pytest.raises(KeyError):
        AzureStorageBroker("azure://doesnt_exist/fake_UUID")
