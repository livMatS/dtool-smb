import datetime
import hashlib
import io
import json
import logging
import os
import socket
import getpass

from base64 import b64encode

try:
    from urlparse import urlunparse
except ImportError:
    from urllib.parse import urlunparse

from smb.SMBConnection import SMBConnection
from smb.base import OperationFailure
from smb.smb_constants import ATTR_DIRECTORY, ATTR_NORMAL
from nmb.NetBIOS import NetBIOS

from dtoolcore.storagebroker import BaseStorageBroker, DiskStorageBroker

from dtoolcore.filehasher import FileHasher, md5sum_hexdigest, md5sum_digest
from dtoolcore.storagebroker import StorageBrokerOSError
from dtoolcore.utils import (
    generate_identifier,
    get_config_value,
    generous_parse_uri,
    mkdir_parents,
    timestamp,
    DEFAULT_CACHE_PATH,
)

from dtool_smb import __version__


logger = logging.getLogger(__name__)


_STRUCTURE_PARAMETERS = {
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


class SMBStorageBroker(BaseStorageBroker):

    #: Attribute used to define the type of storage broker.
    key = "smb"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    # Attribute used to define the structure of the dataset.
    _structure_parameters = _STRUCTURE_PARAMETERS

    # Attribute used to document the structure of the dataset.
    _dtool_readme_txt = _DTOOL_README_TXT

    # Encoding
    _encoding = 'utf-8'

    def __init__(self, uri, config_path=None):

        parse_result = generous_parse_uri(uri)

        self.config_name = parse_result.netloc
        uuid = parse_result.path[1:]

        self.uuid = uuid

        # Connect to SMB server.
        self.conn, self.service_name, self.path = \
            SMBStorageBroker._connect(uri, config_path)

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

        # Define some essential directories to be created.
        self._essential_subdirectories = [
            self._generate_path("dtool_directory"),
            self._data_path,
            self._overlays_path,
            self._annotations_path,
            self._tags_path,
        ]

        # Cache for file hashes computed on upload
        self._hash_cache = {}

        self._smb_cache_abspath = get_config_value(
            "DTOOL_CACHE_DIRECTORY",
            config_path=config_path,
            default=DEFAULT_CACHE_PATH
        )

    def _count_calls(func):
        def wrapper(*args, **kwargs):
            wrapper.num_calls += 1
            return func(*args, **kwargs)
        wrapper.num_calls = 0
        return wrapper

    @classmethod
    @_count_calls
    def _connect(cls, uri, config_path):
        parse_result = generous_parse_uri(uri)

        config_name = parse_result.netloc

        username = get_config_value(
            "DTOOL_SMB_USERNAME_{}".format(config_name),
            config_path=config_path
        )
        server_name = get_config_value(
            "DTOOL_SMB_SERVER_NAME_{}".format(config_name),
            config_path=config_path
        )
        server_port = get_config_value(
            "DTOOL_SMB_SERVER_PORT_{}".format(config_name),
            config_path=config_path
        )
        domain = get_config_value(
            "DTOOL_SMB_DOMAIN_{}".format(config_name),
            config_path=config_path
        )
        service_name = get_config_value(
            "DTOOL_SMB_SERVICE_NAME_{}".format(config_name),
            config_path=config_path
        )
        path = get_config_value(
            "DTOOL_SMB_PATH_{}".format(config_name),
            config_path=config_path
        )

        if not username:
            raise KeyError("No username specified for service '{name}', "
                           "please set DTOOL_SMB_USERNAME_{name}."
                           .format(name=config_name))
        if not server_name:
            raise KeyError("No server name specified for service '{name}', "
                           "please set DTOOL_SMB_SERVER_NAME_{name}."
                           .format(name=config_name))
        if not server_port:
            raise KeyError("No server port specified for service '{name}', "
                           "please set DTOOL_SMB_SERVER_PORT_{name}."
                           .format(name=config_name))
        if not domain:
            raise KeyError("No domain specified for service '{name}', "
                           "please set DTOOL_SMB_DOMAIN_{name}."
                           .format(name=config_name))
        if not service_name:
            raise KeyError("No service name specified for service '{name}', "
                           "please set DTOOL_SMB_SERVICE_NAME_{name}. "
                           "(The service name is the name of the 'share'.)"
                           .format(name=config_name))
        if not path:
            raise KeyError("No path specified for service '{name}', "
                           "please set DTOOL_SMB_PATH_{name}."
                           .format(name=config_name))

        # server_port might be string, i.e. if specified via env vars
        if not isinstance(server_port, int):
            server_port = int(server_port)

        server_ip = socket.gethostbyname(server_name)
        host_name = socket.gethostname()
        password = get_config_value(
            "DTOOL_SMB_PASSWORD_{}".format(config_name),
            config_path=config_path
        )
        if password is None:
            if cls._connect.num_calls == 1:
                password = getpass.getpass()
                cls.password = password
            else:
                password = cls.password
        conn = SMBConnection(username, password, host_name, server_name,
            domain=domain, use_ntlm_v2=True, is_direct_tcp=True)

        logger.info( ( "Connecting from '{host:s}' to "
            "'smb://{user:s}@{ip:s}({server:s}):{port:d}', "
            "DOMAIN '{domain:s}'").format(user=username,
                ip=server_ip, server=server_name,
                port=server_port, host=host_name,
                domain=domain) )

        # for testing, see types of arguments
        logger.debug( ( "Types HOST '{host:s}', USER '{user:s}', IP '{ip:s}', "
           "SERVER '{server:s}', PORT '{port:s}', DOMAIN '{domain:s}'").format(
                user=type(username).__name__,
                ip=type(server_ip).__name__,
                server=type(server_name).__name__,
                port=type(server_port).__name__,
                host=type(host_name).__name__,
                domain=type(domain).__name__))

        conn.connect(server_ip, port=server_port)

        return conn, service_name, path

    # Generic helper functions.

    def _generate_path(self, structure_dict_key):
        logger.debug("_generate_path, structure_dict_key='{}'"
            .format(structure_dict_key))
        logger.debug("_generate_path, self.path='{}', self.uuid='{}', {}"
            .format(self.path, self.uuid,
                self._structure_parameters[structure_dict_key]))
        return os.path.join(self.path, self.uuid,
            *self._structure_parameters[structure_dict_key])

    def _fpath_from_handle(self, handle):
        return os.path.join(self._data_path, handle)

    def _handle_to_fragment_prefixpath(self, handle):
        stem = generate_identifier(handle)
        logger.debug("_handle_to_fragment_prefixpath, handle='{}', stem='{}'"
            .format(handle, stem))
        return os.path.join(self._metadata_fragments_path, stem)

    def _path_exists(self, path):
        try:
            self.conn.getAttributes(self.service_name, path)
        except OperationFailure:
            return False
        return True

    def _create_directory(self, path):
        paths = []
        while not self._path_exists(path):
            paths += [path]
            path = os.path.dirname(path)
        while len(paths) > 0:
            path = paths.pop()
            logger.debug("_create_directory, path = '{}'".format(path))
            self.conn.createDirectory(self.service_name, path)

    def _delete_directory(self, path):
        if not self._path_exists(path):
            return
        for f in self.conn.listPath(self.service_name, path):
            if f.filename != '.' and f.filename != '..':
                fpath = os.path.join(path, f.filename)
                if f.file_attributes & ATTR_DIRECTORY:
                    self._delete_directory(fpath)
                else:
                    self.conn.deleteFiles(self.service_name, fpath)
        self.conn.deleteDirectory(self.service_name, path)

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

        conn, service_name, path = \
            SMBStorageBroker._connect(base_uri, config_path)

        files = conn.listPath(service_name, path)

        uri_list = []
        for f in files:
            if f.filename != '.' and f.filename != '..':
                if f.file_attributes & ATTR_DIRECTORY:
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
        logger.debug("get_text, key='{}'".format(key))
        f = io.BytesIO()
        self.conn.retrieveFile(self.service_name, key, f)
        return f.getvalue().decode(self._encoding)

    def put_text(self, key, text):
        """Put the text into the storage associated with the key."""
        logger.debug("put_text, key='{}', text='{}'".format(key, text))
        parent_directory = os.path.dirname(key)
        self._create_directory(parent_directory)

        with io.BytesIO(text.encode(self._encoding)) as f:
            self.conn.storeFile(self.service_name, key, f)

    def delete_key(self, key):
        """Delete the file/object associated with the key."""
        if self._path_exists(key):
            self.conn.deleteFiles(self.service_name, key)

    def get_size_in_bytes(self, handle):
        """Return the size in bytes."""
        fpath = self._fpath_from_handle(handle)
        return self.conn.getAttributes(self.service_name, fpath).file_size

    def get_utc_timestamp(self, handle):
        """Return the UTC timestamp."""
        fpath = self._fpath_from_handle(handle)
        datetime_obj = datetime.datetime.utcfromtimestamp(
            self.conn.getAttributes(self.service_name, fpath).last_write_time
        )
        return timestamp(datetime_obj)

    def get_hash(self, handle):
        """Return the hash."""
        logger.debug("get_hash, handle='{}'".format(handle))
        logger.debug("get_hash, hash_cache={}".format(self._hash_cache))
        fpath = self._fpath_from_handle(handle)
        logger.debug("get_hash, fpath='{}'".format(fpath))
        try:
            return self._hash_cache[fpath]
        except KeyError:
            logger.debug("get_hash, fpath not found in cache")
            hasher = hashlib.md5()
            with io.BytesIO() as f:
                self.conn.retrieveFile(self.service_name, fpath, f)
                hasher.update(f.getvalue())
            h = hasher.hexdigest()
            self._hash_cache[fpath] = h
            return h

    def has_admin_metadata(self):
        """Return True if the administrative metadata exists.

        This is the definition of being a "dataset".
        """
        return self._path_exists(self.get_admin_metadata_key())

    def _list_names(self, path):
        names = []
        for shf in self.conn.listPath(self.service_name, path):
            if not shf.file_attributes & ATTR_DIRECTORY:
                name, ext = os.path.splitext(shf.filename)
                names.append(name)
        return names

    def list_overlay_names(self):
        """Return list of overlay names."""
        return self._list_names(self._overlays_path)

    def list_annotation_names(self):
        """Return list of annotation names."""
        return self._list_names(self._annotations_path)

    def list_tags(self):
        """Return list of tags."""
        return self._list_names(self._tags_path)

    def get_item_abspath(self, identifier):
        """Return absolute path at which item content can be accessed.

        :param identifier: item identifier
        :returns: absolute path from which the item content can be accessed
        """
        logger.debug("Get item abspath {} {}".format(identifier, self))

        if not hasattr(self, "_admin_metadata_cache"):
            self._admin_metadata_cache = self.get_admin_metadata()
        admin_metadata = self._admin_metadata_cache

        uuid = admin_metadata["uuid"]
        # Create directory for the specific dataset.
        dataset_cache_abspath = os.path.join(self._smb_cache_abspath, uuid)
        mkdir_parents(dataset_cache_abspath)

        manifest = self.get_manifest()
        item = manifest["items"][identifier]
        relpath = item["relpath"]
        _, ext = os.path.splitext(relpath)

        smb_fpath = self._fpath_from_handle(relpath)

        local_item_abspath = os.path.join(
            dataset_cache_abspath,
            identifier + ext
        )
        if not os.path.isfile(local_item_abspath):

            tmp_local_item_abspath = local_item_abspath + ".tmp"

            logger.debug("Retrieving file {} from {}" \
                .format(smb_fpath, self.service_name))
            with open(tmp_local_item_abspath, 'wb') as f:
                attr, size = self.conn.retrieveFile(self.service_name, smb_fpath, f)
            os.rename(tmp_local_item_abspath, local_item_abspath)

        return local_item_abspath

    def _create_structure(self):
        """Create necessary structure to hold a dataset."""
        uuid_path = os.path.join(self.path, self.uuid)

        # Ensure that the specified path does not exist and create it.
        if self._path_exists(uuid_path):
            raise StorageBrokerOSError(
                "Path '{}' already exists on share '{}'.".format(uuid_path,
                    self.service_name))

        logger.debug(
            "_create_structure, creating directory '{}' on share '{}'." \
            .format(os.path.join(self.path, self.uuid), self.service_name))
        self._create_directory(uuid_path)

        # Create more essential subdirectories.
        for abspath in self._essential_subdirectories:
            logger.debug(
                "_create_structure, creating directory '{}' on share '{}'." \
                .format(abspath, self.service_name))
            self._create_directory(abspath)

    def put_item(self, fpath, relpath):
        """Put item with content from fpath at relpath in dataset.

        Missing directories in relpath are created on the fly.

        :param fpath: path to the item on disk
        :param relpath: relative path name given to the item in the dataset as
                        a handle, i.e. a Unix-like relpath
        :returns: the handle given to the item
        """

        logger.debug("put_item, fpath='{}', relpath='{}'".format(fpath,
            relpath))

        # Define the destination path and make any missing parent directories.
        dest_path = os.path.join(self._data_path, relpath)
        dirname = os.path.dirname(dest_path)
        self._create_directory(dirname)

        # Copy the file across.
        self.conn.storeFile(self.service_name, dest_path, open(fpath, 'rb'))

        # Compute hash and store to cache
        self._hash_cache[dest_path] = SMBStorageBroker.hasher(fpath)

        return relpath

    def iter_item_handles(self, path=None):
        """Return iterator over item handles."""

        if path is None:
            path = self._data_path
        relpaths = [None]

        while len(relpaths) > 0:
            relpath = relpaths.pop()
            logger.debug("iter_item_handles, path='{}', relpath='{}'"
                .format(path, relpath))
            if relpath is None:
                fullpath = path
            else:
                fullpath = os.path.join(path, relpath)
            for shf in self.conn.listPath(self.service_name, fullpath):
                logger.debug("iter_item_handles, shf.filename='{}', DIRECTORY={}"
                    .format(shf.filename, shf.file_attributes & ATTR_DIRECTORY))
                if shf.filename != '.' and shf.filename != '..':
                    if relpath is None:
                        new_relpath = shf.filename
                    else:
                        new_relpath = os.path.join(relpath, shf.filename)
                    if shf.file_attributes & ATTR_DIRECTORY:
                        relpaths.append(new_relpath)
                    else:
                        yield new_relpath

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """
        self._create_directory(self._metadata_fragments_path)

        prefix = self._handle_to_fragment_prefixpath(handle)
        logger.debug("add_item_metadata, prefix='{}'".format(prefix))
        fpath = prefix + '.{}.json'.format(key)

        with io.BytesIO(json.dumps(value).encode(self._encoding)) as f:
            self.conn.storeFile(self.service_name, fpath, f)

    def get_item_metadata(self, handle):
        """Return dictionary containing all metadata associated with handle.

        In other words all the metadata added using the ``add_item_metadata``
        method.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :returns: dictionary containing item metadata
        """

        try:
            if not self.conn.getAttributes(self.service_name,
                self._metadata_fragments_path).file_attributes & ATTR_DIRECTORY:
                return {}
        except OperationFailure:
            return {}

        prefix = self._handle_to_fragment_prefixpath(handle)
        logger.debug("get_item_metadata, prefix='{}'".format(prefix))

        def list_paths(dirname):
            for shf in self.conn.listPath(self.service_name, dirname):
                if not shf.file_attributes & ATTR_DIRECTORY:
                    yield os.path.join(dirname, shf.filename)

        files = [f for f in list_paths(self._metadata_fragments_path)
                 if f.startswith(prefix)]

        metadata = {}
        for filename in files:
            key = filename.split('.')[-2]  # filename: identifier.key.json
            with io.BytesIO() as f:
                self.conn.retrieveFile(self.service_name, filename, f)
                value = json.loads(f.getvalue().decode(self._encoding))
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
        logger.debug('pre_freeze_hook, allowed = {}'.format(allowed))
        for d in self.conn.listPath(self.service_name,
            os.path.join(self.path, self.uuid)):
            logger.debug("pre_freeze_hook, d.filename='{}'".format(d.filename))
            if d.file_attributes & ATTR_NORMAL and d.filename not in allowed:
                raise SMBStorageBrokerValidationWarning("Rogue content in base "
                    "of dataset: {}".format(d.filename))

    def post_freeze_hook(self):
        """Post :meth:`dtoolcore.ProtoDataSet.freeze` cleanup actions.

        This method is called at the end of the
        :meth:`dtoolcore.ProtoDataSet.freeze` method.

        In the :class:`dtoolcore.storage_broker.DiskStorageBroker` it removes
        the temporary directory for storing item metadata fragment files.
        """
        self._delete_directory(self._metadata_fragments_path)

    def _list_historical_readme_keys(self):
        historical_readme_keys = []
        uuid_path = os.path.join(self.path, self.uuid)
        for shf in self.conn.listPath(self.service_name, uuid_path):
            logger.debug('_list_historical_readme_keys, {}'.format(shf.filename))
            if shf.filename.startswith("README.yml-"):
                key = os.path.join(uuid_path, shf.filename)
                historical_readme_keys.append(key)
        return historical_readme_keys
