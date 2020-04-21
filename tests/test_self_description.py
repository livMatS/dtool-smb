"""Test the S3StorageBroker self description metadata."""

from . import tmp_uuid_and_uri  # NOQA
from . import (
    _key_exists_in_storage_broker,
    _get_data_structure_from_key,
    _get_unicode_from_key
)


def test_writing_of_dtool_structure_file(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtool_azure import __version__

    # Create a proto dataset.
    uuid, dest_uri = tmp_uuid_and_uri
    name = "test_dtool_structure_file"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None
    )
    proto_dataset.create()

    # Check that the ".dtool/structure.json" file exists.
    expected_azure_key = 'structure.json'
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        expected_azure_key
    )

    expected_content = {
        'http_manifest_key': 'http_manifest.json',
        'fragments_key_prefix': 'fragments/',
        'overlays_key_prefix': 'overlays/',
        'structure_dict_key': 'structure.json',
        'annotations_key_prefix': 'annotations/',
        'tags_key_prefix': 'tags/',
        'admin_metadata_key': 'dtool',
        'storage_broker_version': __version__,
        'dtool_readme_key': 'README.txt',
        'manifest_key': 'manifest.json',
        'dataset_readme_key': 'README.yml'
    }

    actual_content = _get_data_structure_from_key(
        proto_dataset._storage_broker,
        expected_azure_key
    )
    print(actual_content)
    assert expected_content == actual_content


def test_writing_of_dtool_readme_file(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata

    # Create a proto dataset.
    uuid, dest_uri = tmp_uuid_and_uri
    name = "test_dtool_readme_file"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid
    proto_dataset = ProtoDataSet(
        uri=dest_uri,
        admin_metadata=admin_metadata,
        config_path=None
    )
    proto_dataset.create()

    # Check that the ".dtool/README.txt" file exists.
    expected_azure_key = 'README.txt'
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        expected_azure_key
    )

    actual_content = _get_unicode_from_key(
        proto_dataset._storage_broker,
        expected_azure_key
    )
    assert actual_content.startswith("README")
