"""Test the S3StorageBroker self description metadata."""

from . import tmp_uuid_and_uri  # NOQA
from . import (
    _key_exists_in_storage_broker,
    _get_data_structure_from_key,
    _get_unicode_from_key
)


def test_writing_of_dtool_structure_file(tmp_uuid_and_uri):  # NOQA
    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtool_smb import __version__

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

    # Check that the "_dtool/structure.json" file exists.
    expected_smb_key = '_dtool/structure.json'
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        expected_smb_key
    )

    expected_content = {
        "data_directory": ["data"],
        "dataset_readme_relpath": ["README.yml"],
        "dtool_directory": ["_dtool"],
        "admin_metadata_relpath": ["_dtool", "dtool"],
        "structure_metadata_relpath": ["_dtool", "structure.json"],
        "dtool_readme_relpath": ["_dtool", "README.txt"],
        "manifest_relpath": ["_dtool", "manifest.json"],
        "overlays_directory": ["_dtool", "overlays"],
        "annotations_directory": ["_dtool", "annotations"],
        "tags_directory": ["_dtool", "tags"],
        "metadata_fragments_directory": ["_dtool", "tmp_fragments"],
        "storage_broker_version": __version__,
    }

    actual_content = _get_data_structure_from_key(
        proto_dataset._storage_broker,
        expected_smb_key
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

    # Check that the "_dtool/README.txt" file exists.
    assert _key_exists_in_storage_broker(
        proto_dataset._storage_broker,
        '_dtool/README.txt'
    )

    actual_content = _get_unicode_from_key(
        proto_dataset._storage_broker,
        '_dtool/README.txt'
    )
    assert actual_content.startswith("README")
