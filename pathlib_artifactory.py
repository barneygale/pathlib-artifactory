from dataclasses import dataclass
from datetime import datetime
import errno
import io
import posixpath
import stat
from typing import Optional

from requests import Session
from pathlib_abc import PathBase, UnsupportedOperation


def _parse_datetime(date_str):
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    return datetime.fromisoformat(date_str)


@dataclass
class ArtifactoryStat:
    created: datetime
    modified: datetime
    created_by: str
    modified_by: str
    size: int
    is_dir: bool
    children: Optional[list] = None
    md5: Optional[str] = None
    sha1: Optional[str] = None
    sha256: Optional[str] = None
    mime_type: Optional[str] = None

    @classmethod
    def from_storage_response(cls, data):
        checksums = data.get('checksums', {})
        children = None
        if 'children' in data:
            children = [child['uri'][1:] for child in data['children']]
        return cls(
            created=_parse_datetime(data['created']),
            modified=_parse_datetime(data['lastModified']),
            created_by=data['createdBy'],
            modified_by=data['modifiedBy'],
            size=int(data.get('size', 0)),
            is_dir='size' not in data,
            children=children,
            md5=checksums.get('md5'),
            sha1=checksums.get('sha1'),
            sha256=checksums.get('sha256'),
            mime_type=data.get('mimeType'))

    @property
    def st_mode(self):
        return stat.S_IFDIR if self.is_dir else stat.S_IFREG


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
        uri = f'{self.base_uri}/api/storage{self}'
        response = self.session.get(uri)
        if response.status_code == 404:
            raise OSError(errno.ENOENT, 'File not found', str(self))
        response.raise_for_status()
        return ArtifactoryStat.from_storage_response(response.json())

    def open(self, mode='r', buffering=-1, encoding=None,
             errors=None, newline=None):
        if buffering != -1:
            raise UnsupportedOperation(f'Unsupported buffering: {buffering!r}')

        action = ''.join(c for c in mode if c not in 'btU')
        if action == 'r':
            st = self.stat()
            if st.is_dir:
                raise OSError(errno.EISDIR, 'Is a directory', str(self))
            uri = f'{self.base_uri}{self}'
            response = self.session.get(uri, stream=True)
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
        if not st.is_dir:
            raise OSError(errno.ENOTDIR, 'Not a directory', str(self))
        return iter([self.joinpath(name) for name in st.children])

    def absolute(self):
        if self.is_absolute():
            return self
        return self.with_segments(f'/{self}')

    # FIXME: touch
    # FIXME: mkdir
    # FIXME: rename
    # FIXME: replace
    # FIXME: unlink
    # FIXME: rmdir
    # FIXME: owner
    # FIXME: group

    def as_uri(self):
        return self.base_uri + str(self)

    @classmethod
    def from_uri(cls, uri):
        head, tail = uri.split('/artifactory/', 1)
        return cls(f'/{tail}', base_uri=f'{head}/artifactory')
