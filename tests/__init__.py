
import os
import json
import tempfile
import shutil
from contextlib import contextmanager

import pytest

from dtoolcore import generate_admin_metadata

from dtool_smb.storagebroker import (
    SMBStorageBroker,
)

_HERE = os.path.dirname(__file__)
TEST_SAMPLE_DATA = os.path.join(_HERE, "data")

CONFIG_PATH = os.path.expanduser("~/.config/dtool/dtool.json")

SMB_TEST_BASE_URI = os.getenv("SMB_TEST_BASE_URI", "smb://dtooltesting")


@contextmanager
def tmp_env_var(key, value):
    os.environ[key] = value
    yield
    del os.environ[key]


@contextmanager
def tmp_directory():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


def _key_exists_in_storage_broker(storage_broker, key):
    fpath = os.path.join(
        os.path.join(storage_broker.path, storage_broker.uuid),
        key
    )
    return storage_broker._path_exists(fpath)


def _get_data_structure_from_key(storage_broker, key):
    fpath = os.path.join(
        os.path.join(storage_broker.path, storage_broker.uuid),
        key
    )
    return json.loads(storage_broker.get_text(fpath))


def _get_unicode_from_key(storage_broker, key):
    fpath = os.path.join(
        os.path.join(storage_broker.path, storage_broker.uuid),
        key
    )
    return storage_broker.get_text(fpath)


def _remove_dataset(uri):

    storage_broker = SMBStorageBroker(uri, config_path=CONFIG_PATH)

    # FIXME! Add deletion of dataset
    storage_broker._delete_directory(
        os.path.join(storage_broker.path, storage_broker.uuid))


@pytest.fixture
def tmp_uuid_and_uri(request):
    admin_metadata = generate_admin_metadata("test_dataset")
    uuid = admin_metadata["uuid"]

    uri = SMBStorageBroker.generate_uri(
        "test_dataset",
        uuid,
        SMB_TEST_BASE_URI
    )

    @request.addfinalizer
    def teardown():
        _remove_dataset(uri)

    return (uuid, uri)


@pytest.fixture
def tmp_dir_fixture(request):
    d = tempfile.mkdtemp()

    @request.addfinalizer
    def teardown():
        shutil.rmtree(d)
    return d
