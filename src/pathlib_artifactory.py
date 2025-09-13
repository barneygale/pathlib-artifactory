import contextlib
import errno
import posixpath

from pathlib_abc import PathInfo, ReadablePath, vfspath
from requests import Session
from requests_seekable import SeekableResponse


__version__ = '0.4.1'

session = Session()


class ArtifactoryPathInfo(PathInfo):
    __slots__ = ('_uri', '_exists', '_is_dir', '_size', '_children')

    def __init__(self, uri, exists=None, is_dir=None):
        self._uri = uri
        self._exists = exists
        self._is_dir = is_dir
        self._size = None
        self._children = None

    def _query(self):
        response = session.get(self._uri)
        if response.status_code == 404:
            self._exists = False
        else:
            response.raise_for_status()
            data = response.json()
            self._exists = True
            self._is_dir = 'size' not in data
            self._size = 0 if self._is_dir else int(data['size'])
            self._children = data.get('children', [])

    def exists(self, *, follow_symlinks=True):
        if self._exists is None:
            self._query()
        return self._exists

    def is_dir(self, *, follow_symlinks=True):
        if not self.exists(follow_symlinks=follow_symlinks):
            return False
        elif self._is_dir is None:
            self._query()
        return self._is_dir

    def is_file(self, *, follow_symlinks=True):
        if not self.exists(follow_symlinks=follow_symlinks):
            return False
        elif self._is_dir is None:
            self._query()
        return not self._is_dir

    def is_symlink(self):
        return False

    def size(self):
        if self._size is None:
            self._query()
        return self._size

    def children(self):
        if self._children is None:
            self._query()
        for child in self._children:
            name = child['uri']
            is_dir = child['folder']
            info = type(self)(self._uri + name, exists=True, is_dir=is_dir)
            yield name, info


class ArtifactoryPath(ReadablePath):
    parser = posixpath
    __slots__ = ('_segments', '_info', 'base_uri')

    def __init__(self, *pathsegments, base_uri, info=None):
        self._segments = pathsegments
        self._info = info
        self.base_uri = base_uri

    def __vfspath__(self):
        return posixpath.join(*self._segments) if self._segments else ''

    def __repr__(self):
        return f'{type(self).__name__}({vfspath(self)!r}, base_uri={self.base_uri!r})'

    def __hash__(self):
        return hash((self.base_uri, vfspath(self)))

    def __eq__(self, other):
        if not isinstance(other, ArtifactoryPath):
            return NotImplemented
        return self.base_uri == other.base_uri and vfspath(self) == vfspath(other)

    def __open_reader__(self):
        response = session.get(self.as_uri(), stream=True)
        if response.status_code == 404:
            raise OSError(errno.ENOENT, 'File not found', vfspath(self))
        response.raise_for_status()
        return contextlib.closing(SeekableResponse(response))

    def with_segments(self, *pathsegments, info=None):
        return type(self)(*pathsegments, base_uri=self.base_uri, info=info)

    @property
    def info(self):
        if self._info is None:
            self._info = ArtifactoryPathInfo(f'{self.base_uri}/api/storage{vfspath(self)}')
        return self._info

    def exists(self, *, follow_symlinks=True):
        """Whether this path exists."""
        return self.info.exists(follow_symlinks=follow_symlinks)

    def is_dir(self, *, follow_symlinks=True):
        """Whether this path is a directory."""
        return self.info.is_dir(follow_symlinks=follow_symlinks)

    def is_file(self, *, follow_symlinks=True):
        """Whether this path is a regular file."""
        return self.info.is_file(follow_symlinks=follow_symlinks)

    def is_symlink(self):
        """Whether this path is a symbolic link."""
        return self.info.is_symlink()

    def iterdir(self):
        if not self._info.exists():
            raise FileNotFoundError(errno.ENOENT, 'File not found', self.as_uri())
        elif not self._info.is_dir():
            raise NotADirectoryError(errno.ENOTDIR, 'Not a directory', self.as_uri())
        else:
            return (self.with_segments(vfspath(self) + uri, info=info)
                    for uri, info in self.info.children())

    def readlink(self):
        raise OSError(errno.EINVAL, 'Not a symlink', self.as_uri())

    def as_uri(self):
        return self.base_uri + vfspath(self)

    @classmethod
    def from_uri(cls, uri):
        head, tail = uri.split('/artifactory/', 1)
        return cls(f'/{tail}', base_uri=f'{head}/artifactory')
