from dataclasses import dataclass
from datetime import datetime
import errno
import io
import posixpath
import stat

from requests import Session
from pathlib_abc import PathBase, UnsupportedOperation


def _parse_datetime(date_str):
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    return datetime.fromisoformat(date_str)


@dataclass
class Status:
    created: datetime
    modified: datetime
    created_by: str
    modified_by: str


@dataclass
class FileStatus(Status):
    size: int
    md5: str
    sha1: str
    sha256: str
    mime_type: str
    st_mode = stat.S_IFREG


@dataclass
class DirectoryStatus(Status):
    children: list
    st_mode = stat.S_IFDIR


class ArtifactoryPath(PathBase):
    parser = posixpath

    def __init__(self, *pathsegments, base_uri, session=None):
        super().__init__(*pathsegments)
        self.base_uri = base_uri
        self.session = session or Session()

    def __repr__(self):
        return f'{type(self).__name__}({str(self)!r}, base_uri={self.base_uri!r})'

    def __hash__(self):
        return hash((self.base_uri, str(self)))

    def __eq__(self, other):
        if not isinstance(other, ArtifactoryPath):
            return NotImplemented
        return self.base_uri == other.base_uri and str(self) == str(other)

    def with_segments(self, *pathsegments):
        return type(self)(*pathsegments, base_uri=self.base_uri, session=self.session)

    def stat(self, *, follow_symlinks=True):
        path = str(self.absolute())
        uri = f'{self.base_uri}/api/storage{path}'
        response = self.session.get(uri)
        if response.status_code == 404:
            raise OSError(errno.ENOENT, 'File not found', path)
        response.raise_for_status()
        data = response.json()
        if 'size' in data:
            return FileStatus(
                created=_parse_datetime(data['created']),
                created_by=data['createdBy'],
                modified=_parse_datetime(data['lastModified']),
                modified_by=data['modifiedBy'],
                size=int(data['size']),
                mime_type=data['mimeType'],
                **data['checksums'])
        else:
            return DirectoryStatus(
                created=_parse_datetime(data['created']),
                created_by=data['createdBy'],
                modified=_parse_datetime(data['lastModified']),
                modified_by=data['modifiedBy'],
                children=[child['uri'][1:] for child in data['children']])

    def open(self, mode='r', buffering=-1, encoding=None,
             errors=None, newline=None):
        if buffering != -1:
            raise UnsupportedOperation(f'Unsupported buffering: {buffering!r}')

        action = ''.join(c for c in mode if c not in 'btU')
        if action == 'r':
            st = self.stat()
            if isinstance(st, DirectoryStatus):
                raise OSError(errno.EISDIR, 'Is a directory', str(self))
            response = self.session.get(self.as_uri(), stream=True)
            if response.status_code == 404:
                raise OSError(errno.ENOENT, 'File not found', str(self))
            response.raise_for_status()
            fileobj = response.raw
        else:
            # FIXME: support 'w' mode.
            raise UnsupportedOperation(f'Unsupported mode: {mode!r}')
        if 'b' not in mode:
            fileobj = io.TextIOWrapper(fileobj, encoding, errors, newline)
        return fileobj

    def iterdir(self):
        st = self.stat()
        if not isinstance(st, DirectoryStatus):
            raise OSError(errno.ENOTDIR, 'Not a directory', str(self))
        return iter([self.joinpath(name) for name in st.children])

    def absolute(self):
        if self.is_absolute():
            return self
        raise UnsupportedOperation(f'Relative path: {self!r}')

    # FIXME: touch
    # FIXME: mkdir
    # FIXME: rename
    # FIXME: replace
    # FIXME: unlink
    # FIXME: rmdir
    # FIXME: owner
    # FIXME: group

    def as_uri(self):
        return self.base_uri + str(self.absolute())

    @classmethod
    def from_uri(cls, uri):
        head, tail = uri.split('/artifactory/', 1)
        return cls(f'/{tail}', base_uri=f'{head}/artifactory')
