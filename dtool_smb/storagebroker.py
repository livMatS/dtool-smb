import hashlib
import io
import json
import os
import socket

from base64 import b64encode

try:
    from urlparse import urlunparse
except ImportError:
    from urllib.parse import urlunparse

from dtoolcore.storagebroker import DiskStorageBroker

from dtoolcore.utils import (
    generate_identifier,
    get_config_value,
    mkdir_parents,
    generous_parse_uri,
    timestamp,
    DEFAULT_CACHE_PATH,
)

from dtoolcore.filehasher import FileHasher, md5sum_hexdigest, md5sum_digest

from smb.SMBConnection import SMBConnection
from smb.base import NotReadyError, NotConnectedError, OpeationFailure
from smb.smb2_constants import SMB2_FILE_ATTRIBUTE_DIRECTORY
from nmb.NetBIOS import NetBIOS


_STRUCTURE_PARAMETERS = {
    "data_directory": ["data"],
    "dataset_readme_relpath": ["README.yml"],
    "dtool_directory": [".dtool"],
    "admin_metadata_relpath": [".dtool", "dtool"],
    "structure_metadata_relpath": [".dtool", "structure.json"],
    "dtool_readme_relpath": [".dtool", "README.txt"],
    "manifest_relpath": [".dtool", "manifest.json"],
    "overlays_directory": [".dtool", "overlays"],
    "annotations_directory": [".dtool", "annotations"],
    "tags_directory": [".dtool", "tags"],
    "metadata_fragments_directory": [".dtool", "tmp_fragments"],
    "storage_broker_version": __version__,
}

_DTOOL_README_TXT = """README
======
This is a Dtool dataset stored in an SMB share.

Content provided during the dataset creation process
----------------------------------------------------

Directory named $UUID, where UUID is the unique identifier for the
dataset.

Dataset descriptive metadata: README.yml

Dataset items. The keys for these blobs are item identifiers. An item
identifier is the sha1sum hexdigest of the relative path used to represent the
file on traditional file system disk.

Administrative metadata describing the dataset is encoded as metadata on the
container.


Automatically generated blobs
-----------------------------

This file: README.txt
Structural metadata describing the dataset: structure.json
Structural metadata describing the data items: manifest.json
Per item descriptive metadata prefixed by: overlays/
Dataset key/value pairs metadata prefixed by: annotations/
Dataset tags metadata prefixed by: tags/
"""


class SMBStorageBrokerValidationWarning(Warning):
    pass


class SMBStorageBroker(DiskStorageBroker):

    #: Attribute used to define the type of storage broker.
    key = "smb"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    # Attribute used to document the structure of the dataset.
    _dtool_readme_txt = _DTOOL_README_TXT

    def __init__(self, uri, config_path=None):

        parse_result = generous_parse_uri(uri)

        self.config_name = parse_result.netloc
        uuid = parse_result.path[1:]

        self.uuid = uuid

        # Define some other more abspaths.
        self._data_path = self._generate_path("data_directory")
        self._overlays_path = self._generate_path("overlays_directory")
        self._annotations_path = self._generate_path(
            "annotations_directory"
        )
        self._tags_path = self._generate_path(
            "tags_directory"
        )
        self._metadata_fragments_path = self._generate_path(
            "metadata_fragments_directory"
        )

        # Connect to SMB server.
        self.conn, self.service_name, self.path = \
            self._connect(uri, config_path)


    def _connect(uri, config_path):
        parse_result = generous_parse_uri(uri)

        config_name = parse_result.netloc

        username = get_config_value(
            "DTOOL_SMB_USERNAME_{}".format(self.config_name)
        )
        password = get_config_value(
            "DTOOL_SMB_PASSWORD_{}".format(self.config_name)
        )
        server_name = get_config_value(
            "DTOOL_SMB_SERVER_NAME_{}".format(self.config_name)
        )
        server_port = get_config_value(
            "DTOOL_SMB_SERVER_PORT_{}".format(self.config_name)
        )
        domain = get_config_value(
            "DTOOL_SMB_DOMAIN_{}".format(self.config_name)
        )
        service_name = get_config_value(
            "DTOOL_SMB_SERVICE_NAME_{}".format(self.config_name)
        )
        path = get_config_value(
            "DTOOL_SMB_PATH_{}".format(self.config_name)
        )

        server_ip = socket.gethostbyname(server_name)
        host_name = socket.gethostname()

        conn = SMBConnection(username, password, host_name, server_name,
            domain=domain, use_ntlm_v2=True, is_direct_tcp=True)

        logger.info( ( "Connecting from '{host:s}' to "
            "'smb://{user:s}@{ip:s}({server:s}):{port:d}', "
            " DOMAIN '{domain:s}'").format(user=username,
                ip=server_ip, server=server_name, 
                port=server_port, host=host_name, 
                domain=domain) )

        # for testing, see types of arguments
        logger.debug( ( "Types HOST '{host:s}', USER '{user:s}', IP '{ip:s}', "
           "SERVER '{server:s}', PORT '{port:s}', DOMAIN '{domain:s}', "
            "PASSWORD '{password:s}'").format(
                user=type(username).__name__, 
                ip=type(server_ip).__name__, 
                server=type(server_name).__name__, 
                port=type(server_port).__name__, 
                host=type(host_name).__name__, 
                domain=type(domain).__name__,
                password=type(password).__name__ ) )

        conn.connect(server_ip, port=server_port)

        return conn, service_name, path

    # Generic helper functions.

    def _generate_path(self, structure_dict_key):
        return os.path.join(self.path, self.uuid,
            *self._structure_parameters[structure_dict_key])

    def _fpath_from_handle(self, handle):
        return os.path.join(self._data_abspath, handle)

    # Class methods to override.

    @classmethod
    def generate_uri(cls, name, uuid, base_uri):
        scheme, netloc, path, _, _, _ = generous_parse_uri(base_uri)
        assert scheme == 'smb'

        # Force path (third component of tuple) to be the dataset UUID
        uri = urlunparse((scheme, netloc, uuid, _, _, _))

        return uri

    @classmethod
    def list_dataset_uris(cls, base_uri, config_path):
        """Return list containing URIs with base URI."""

        conn, service_name, path = self,_connect(base_uri, config_path)

        files = conn.listPath(service_name, path)

        uri_list = []
        for f in files:
            uuid = f.filename

            uri = cls.generate_uri(None, uuid, base_uri)
            uri_list.append(uri)

        return uri_list

    # Methods to override.

    def get_admin_metadata_key(self):
        "Return the path to the admin metadata file."""
        return self._generate_path("admin_metadata_relpath")

    def get_readme_key(self):
        "Return the path to the readme file."""
        return self._generate_path("dataset_readme_relpath")

    def get_manifest_key(self):
        "Return the path to the readme file."""
        return self._generate_path("manifest_relpath")

    def get_structure_key(self):
        "Return the path to the structure parameter file."""
        return self._generate_path("structure_metadata_relpath")

    def get_dtool_readme_key(self):
        "Return the path to the dtool readme file."""
        return self._generate_path("dtool_readme_relpath")

    def get_overlay_key(self, overlay_name):
        "Return the path to the overlay file."""
        return os.path.join(self._overlays_path, overlay_name + '.json')

    def get_annotation_key(self, annotation_name):
        "Return the path to the annotation file."""
        return os.path.join(
            self._annotations_path,
            annotation_name + '.json'
        )

    def get_tag_key(self, tag):
        "Return the path to the tag file."""
        return os.path.join(
            self._tags_path,
            tag
        )

    def get_text(self, key):
        """Return the text associated with the key."""
        f = io.StringIO()
        self.conn.retrieveFile(self.service_name, key, f)
        return f.read()

    def put_text(self, key, text):
        """Put the text into the storage associated with the key."""
        parent_directory = os.path.dirname(key)
        self.conn.createDirectory(self.service_name, parent_directory)

        mkdir_parents(parent_directory)
        f = io.StringIO()
        f.write(text)
        self.conn.storeFile(self.service_name, key, f)

    def delete_key(self, key):
        """Delete the file/object associated with the key."""
        self.conn.deleteFile(self.service_name, key)

    def get_size_in_bytes(self, handle):
        """Return the size in bytes."""
        return self.conn.getAttributes(self.service_name, key).file_size

    def get_utc_timestamp(self, handle):
        """Return the UTC timestamp."""
        fpath = self._fpath_from_handle(handle)
        datetime_obj = datetime.datetime.utcfromtimestamp(
            self.conn.getAttributes(self.service_name, fpath).last_write_time
        )
        return timestamp(datetime_obj)

    def get_hash(self, handle):
        """Return the hash."""
        fpath = self._fpath_from_handle(handle)
        f = io.BytesIO()
        self.conn.retrieveFile(self.service_name, fpath, f)
        hasher = hashlib.md5()
        hasher.update(f)
        return hasher.hexdigest()

    def has_admin_metadata(self):
        """Return True if the administrative metadata exists.

        This is the definition of being a "dataset".
        """
        try:
            self.conn.getAttributes(self.service_name,
                self.get_admin_metadata_key())
        except OpeationFailure:
            return False
        return True

    def _list_names(self, path):
        names = []
        for shf in self.conn.listPath(self.service_name, path):
            name, ext = os.path.splitext(shf.filename)
            names.append(name)
        return names

    def list_overlay_names(self):
        """Return list of overlay names."""
        return self._list_names(self._overlays_path)

    def list_annotation_names(self):
        """Return list of annotation names."""
        return self._list_names(self._annotation_path)

    def list_tags(self):
        """Return list of tags."""
        return self._list_names(self._tags_path)

    def get_item_path(self, identifier):
        """Return absolute path at which item content can be accessed.

        :param identifier: item identifier
        :returns: absolute path from which the item content can be accessed
        """
        manifest = self.get_manifest()
        relpath = hitem["relpath"]
        item_path = os.path.join(self._data_path, relpath)
        return item_path

    def _create_structure(self):
        """Create necessary structure to hold a dataset."""

        # Ensure that the specified path does not exist and create it.
        self.conn.createDirectory(self.service_name, self._path)

        # Create more essential subdirectories.
        for abspath in self._essential_subdirectories:
            self.conn.createDirectory(self.service_name, abspath)

    def put_item(self, fpath, relpath):
        """Put item with content from fpath at relpath in dataset.

        Missing directories in relpath are created on the fly.

        :param fpath: path to the item on disk
        :param relpath: relative path name given to the item in the dataset as
                        a handle, i.e. a Unix-like relpath
        :returns: the handle given to the item
        """

        # Define the destination path and make any missing parent directories.
        dest_path = os.path.join(self._data_path, relpath)
        dirname = os.path.dirname(dest_path)
        self.conn.createDirectory(self.service_name, dirname)

        # Copy the file across.
        self.conn.storeFile(fpath, dest_path)

        return relpath

    def iter_item_handles(self, path=None):
        """Return iterator over item handles."""

        if path is None:
            path = self._data_path

        for shf in self.conn.listPath(self.service_name, path)
            if shf.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY:
                self.iter_item_handles(path=shf.filename)
            yield shf.filename

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """
        self.conn.createDirectory(self.service_name,
            self._metadata_fragments_path)

        prefix = self._handle_to_fragment_absprefixpath(handle)
        fpath = prefix + '.{}.json'.format(key)

        f = io.BytesIO()
        json.dump(value, f)
        self.conn.storeFile(self.service_name, path, f)

    def get_item_metadata(self, handle):
        """Return dictionary containing all metadata associated with handle.

        In other words all the metadata added using the ``add_item_metadata``
        method.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :returns: dictionary containing item metadata
        """

        prefix = self._handle_to_fragment_absprefixpath(handle)

        def list_paths(dirname):
            for shf in self.conn.listPath(self.service_name, dirname):
                yield os.path.join(dirname, shf.filename)

        files = [f for f in list_paths(self._metadata_fragments_path)
                 if f.startswith(prefix)]

        metadata = {}
        for filename in files:
            key = filename.split('.')[-2]  # filename: identifier.key.json
            f = io.StringIO()
            self.conn.retrieveFile(self.service_name, filename, f)
            value = json.load(f)
            metadata[key] = value

        return metadata

    def pre_freeze_hook(self):
        """Pre :meth:`dtoolcore.ProtoDataSet.freeze` actions.

        This method is called at the beginning of the
        :meth:`dtoolcore.ProtoDataSet.freeze` method.

        It may be useful for remote storage backends to generate
        caches to remove repetitive time consuming calls
        """
        allowed = set([v[0] for v in _STRUCTURE_PARAMETERS.values()])
        for d in self.conn.listPath(self.service_name, self._path):
            if d.filename not in allowed:
                msg = "Rogue content in base of dataset: {}".format(d.filename)
                raise(SMBStorageBrokerValidationWarning(msg))

    def post_freeze_hook(self):
        """Post :meth:`dtoolcore.ProtoDataSet.freeze` cleanup actions.

        This method is called at the end of the
        :meth:`dtoolcore.ProtoDataSet.freeze` method.

        In the :class:`dtoolcore.storage_broker.DiskStorageBroker` it removes
        the temporary directory for storing item metadata fragment files.
        """
        self.conn.deleteFiles(self.service_name, self._metadata_fragments_path)

    def _list_historical_readme_keys(self):
        historical_readme_keys = []
        for shf in self.conn.listPaths(self.service_name, self._path):
            if shf.filename.startswith("README.yml-"):
                key = os.path.join(self._path, shf.filename)
                historical_readme_keys.append(key)
        return historical_readme_keys
