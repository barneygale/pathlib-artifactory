API Reference
=============

.. module:: pathlib_artifactory

.. class:: ArtifactoryPath(*pathsegments, base_uri, session=None)

    Class that represents an artifact path on an Artifactory server.

    The path is formed by joining the given *pathsegments* together with
    slashes. The root URI of the artifactory server (usually ending
    "``/artifactory``") should be given as a named *base_uri* argument.

    A ``requests.Session`` object may be given as *session*; this is useful
    for pre-configuring HTTP headers, e.g. for authentication.
