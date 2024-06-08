import dataclasses
import errno
import io
import posixpath
import stat

from requests import Session
from pathlib_abc import PathBase, UnsupportedOperation


@dataclasses.dataclass
class ArtifactoryStat:
    st_mode: int
    st_size: int
    st_children: list


class ArtifactoryPath(PathBase):
    parser = posixpath

    def __init__(self, *pathsegments, base_uri, session=None):
        super().__init__(*pathsegments)
        self.base_uri = base_uri
        self.session = session or Session()

    def __repr__(self):
        return f"{type(self).__name__}({str(self)!r}, base_uri={self.base_uri!r})"

    def __hash__(self):
        return hash((self.base_uri, str(self)))

    def __eq__(self, other):
        if not isinstance(other, ArtifactoryPath):
            return NotImplemented
        return self.base_uri == other.base_uri and str(self) == str(other)

    def with_segments(self, *pathsegments):
        return type(self)(*pathsegments, base_uri=self.base_uri, session=self.session)

    def stat(self, *, follow_symlinks=True):
        uri = self.base_uri + "/api/storage" + str(self)
        response = self.session.get(uri)
        if response.status_code == 404:
            raise OSError(errno.ENOENT, "File not found", str(self))
        response.raise_for_status()
        response = response.json()
        if 'size' in response:
            return ArtifactoryStat(stat.S_IFREG, int(response['size']), [])
        else:
            return ArtifactoryStat(stat.S_IFDIR, 0, response['children'])

    def open(self, mode='r', buffering=-1, encoding=None,
             errors=None, newline=None):
        if buffering != -1:
            raise UnsupportedOperation(f'Unsupported buffering: {buffering!r}')
        uri = f'{self.base_uri}{self}'
        action = ''.join(c for c in mode if c not in 'btU')
        if action == 'r':
            response = self.session.get(uri, stream=True)
            if response.status_code == 404:
                raise OSError(errno.ENOENT, "File not found", str(self))
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
        for child in st.st_children:
            yield self.joinpath(child['uri'][1:])

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
