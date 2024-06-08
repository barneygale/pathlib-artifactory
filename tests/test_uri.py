from pathlib_artifactory import ArtifactoryPath

def test_from_uri():
    def check(uri, pathstr):
        p = ArtifactoryPath.from_uri(uri)
        assert p.base_uri == 'http://artifactory:8080/artifactory'
        assert str(p) == pathstr

    check('http://artifactory:8080/artifactory/repo', '/repo')
    check('http://artifactory:8080/artifactory/repo/', '/repo/')
    check('http://artifactory:8080/artifactory/repo/dir', '/repo/dir')
    check('http://artifactory:8080/artifactory/repo/dir/', '/repo/dir/')
    check('http://artifactory:8080/artifactory/repo/dir/file', '/repo/dir/file')

def test_as_uri():
    base_uri = 'http://artifactory:8080/artifactory'
    def check(uri, pathstr):
        p = ArtifactoryPath(pathstr, base_uri=base_uri)
        assert p.as_uri() == uri

    check('http://artifactory:8080/artifactory/repo', '/repo')
    check('http://artifactory:8080/artifactory/repo/', '/repo/')
    check('http://artifactory:8080/artifactory/repo/dir', '/repo/dir')
    check('http://artifactory:8080/artifactory/repo/dir/', '/repo/dir/')
    check('http://artifactory:8080/artifactory/repo/dir/file', '/repo/dir/file')