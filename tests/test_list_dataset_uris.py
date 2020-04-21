import os

from . import tmp_uuid_and_uri  # NOQA
from . import TEST_SAMPLE_DATA
from . import CONFIG_PATH


def test_list_dataset_uris(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    sample_data_path = os.path.join(TEST_SAMPLE_DATA)
    local_file_path = os.path.join(sample_data_path, 'tiny.png')

    # Create a minimal dataset
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=CONFIG_PATH)
    proto_dataset.create()
    proto_dataset.put_item(local_file_path, 'tiny with space.png')
    proto_dataset.freeze()

    from dtool_azure.storagebroker import AzureStorageBroker
    assert len(AzureStorageBroker.list_dataset_uris(
        dest_uri,
        CONFIG_PATH)
    ) > 0
