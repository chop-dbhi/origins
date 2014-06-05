from __future__ import unicode_literals

import os
import json
import time
import logging
import tempfile
import subprocess
import requests
from urlparse import urlparse
from . import logger
from .model import COMPONENT_TYPES, Bundle
from .constants import NAMESPACES
from .writers import Writer, Neo4jWriter, StreamWriter  # noqa


try:
    str = unicode
except NameError:
    pass


class InvalidProvenance(Exception):
    pass


def qualify(props, namespaces, default=None):
    "Qualifies keys and string values using the provided namespaces."
    copy = {}

    # Convert all keys into qualified names
    for key, value in props.items():
        if isinstance(key, bytes):
            key = key.decode('utf8')

        try:
            key = namespaces.qname(key, default=default)
        except ValueError:
            logger.debug('cannot qualify property key: {}'.format(key))

        # Handle PROV-JSON data typing: http://www.w3.org/Submission/2013/SUBM-prov-json-20130424/#data-typing  # noqa
        # TODO convert to primitive value if supported
        if isinstance(value, dict) and '$' in value:
            value = value['$']

        if isinstance(value, bytes):
            value = value.decode('utf8')

        if isinstance(value, str):
            try:
                value = namespaces.qname(value, default=default)
            except ValueError:
                logger.debug('cannot qualify property value: {}'.format(value))

        copy[key] = value

    return copy


def parse_component(comp_id, comp_content, namespaces, default, timestamp):
    # Qualify the component identifier. Since this is part of the
    # PROV-JSON spec, the default namespace is `prov`.
    comp_name = namespaces.qname(comp_id, default='prov')

    # Get the corresponding component type
    try:
        comp_type = COMPONENT_TYPES[comp_name.namespace][comp_name]
    except KeyError:
        logger.error('no component type for {}'.format(comp_name))
        raise

    descriptions = []

    # Initialize component instance for each type
    for desc_id, desc_attrs in comp_content.items():
        desc_name = namespaces.qname(desc_id, default=default)

        # Bundles do not have attributes of their own
        if comp_type is Bundle:
            qualified_props = {}
        else:
            qualified_props = qualify(desc_attrs, namespaces, default=default)

        instance = comp_type(qualified_props, timestamp=timestamp)
        descriptions.append((desc_name, instance))

    return descriptions


def parse_container(prov_data, namespaces, timestamp=None, default=None):

    descriptions = []

    # Since they are the most dependent on, queue up first
    for comp_id in ('entity', 'activity', 'agent'):
        if comp_id not in prov_data:
            continue

        comp_content = prov_data.pop(comp_id)
        descriptions.extend(parse_component(comp_id, comp_content, namespaces,
                                            default, timestamp))

    # Iterate over component identifiers and initialize using the corresponding
    # component types.
    for comp_id, comp_content in prov_data.items():
        descriptions.extend(parse_component(comp_id, comp_content, namespaces,
                                            default, timestamp))

    return descriptions


def parse_prov_data(prov_data):
    timestamp = int(time.time() * 1000)

    prefix = prov_data.pop('prefix', None)
    namespaces = NAMESPACES.extend(prefix)
    default = 'default' in prefix and 'default' or None

    bundles = {}

    descriptions = parse_container(prov_data, namespaces,
                                   timestamp=timestamp,
                                   default=default)

    bundles[None] = descriptions

    for bundle_id, bundle_content in prov_data.get('bundle', {}).items():
        bundle_name = namespaces.qname(bundle_id, default=default)

        prefix = bundle_content.pop('prefix', None)

        if prefix:
            bundle_namespaces = namespaces.extend(prefix)
        else:
            bundle_namespaces = namespaces

        bundles[bundle_name] = parse_container(bundle_content,
                                               bundle_namespaces,
                                               timestamp=timestamp,
                                               default=default)

    return bundles


def read_document(url, type=None):
    """Reads or fetchs a PROV document.

    This currently relies on the ProvToolbox to convert the input to the
    PROV-JSON format in the file is in PROV-X, PROV-O, or PROV-N.
    """
    # Remote file, attempt to fetch it
    if urlparse(url).scheme:
        if type:
            ext = '.' + type
        else:
            ext = os.path.splitext(url)[1]

            if not ext:
                raise TypeError('file type not specified and URL does not '
                                'have an extension')

        resp = requests.get(url, stream=True)

        fd, inpath = tempfile.mkstemp(suffix=ext)

        for chunk in resp.iter_content(512, decode_unicode=True):
            os.write(fd, chunk)

        # Flush buffer to file
        os.fsync(fd)
    else:
        inpath = url

    if inpath.endswith('.json'):
        with open(inpath) as f:
            data = json.load(f)
    else:
        fd, outpath = tempfile.mkstemp(suffix='.json')

        try:
            output = subprocess.check_call([
                'provconvert',
                '-infile', inpath,
                '-outfile', outpath,
            ])
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                raise Exception('provconvert not found on path. ProvToolbox '
                                'must be installed to convert from other '
                                'PROV representations.')
        except subprocess.CalledProcessError:
            raise Exception('error converting to PROV-JSON')

        logging.debug(output)

        with os.fdopen(fd) as f:
            data = json.load(f)

    return data


def load_bundle(bundle, descriptions, writer):
    for name, instance in descriptions:
        writer.add(name, instance, bundle)


def load_document(data, writer, type=None):
    "Loads prov data or from a file."
    if isinstance(data, (str, bytes)):
        data = read_document(data, type=type)

    bundles = parse_prov_data(data)

    for bundle, descriptions in bundles.items():
        load_bundle(bundle, descriptions, writer)
