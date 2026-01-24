# Python API Release

## Releasing the `datacommons_client` package
Support for V2 of the Data Commons API is being released as a new client library
called `datacommons_client`.

To release:
1. Update [CHANGELOG.md](../CHANGELOG.md) with relevant changes.
2. Bump the version by running `hatch version` followed by `patch`, `minor`, `major`, a 
specific version number, or `--pre beta` for a beta version, for example.
3. Build the package
```bash
hatch build
```
4. (optionally) Test the deployment process locally
```bash
hatch run release:localtest
```
5. Test the deployment process on Test PyPi
```bash
hatch run release:testpypi
```

6. Once verified, upload to PyPI:
```bash
hatch run release:pypi
```

7. Create a version tag on Git:
```bash
hatch run release:tag
```
