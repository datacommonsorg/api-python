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

---

## Releasing the legacy packages


Note: Always release `datacommons_pandas` when `datacommons` is released.

**If this is your first time releasing to PyPI**, please review the PyPI guide
starting from the
[setup
section](https://packaging.python.org/tutorials/packaging-projects/#creating-setup-py).

## Prepare release tools

```bash
python3 -m venv .env
source .env/bin/activate
python3 -m pip install --upgrade setuptools wheel
python3 -m pip install --upgrade twine
```

## Release to Test PyPI

1. In [datacommons/setup.py](../datacommons/setup.py) and [datacommons_pandas/setup.py](../datacommons_pandas/setup.py):

   - Append "-USERNAME" to the package "NAME". For example,
     `NAME = 'foo_package-janedoe123'`.
   - Increment the "VERSION" codes to something that has not been used in your
     test project. This will not affect the production PyPI versioning.

1. In the repo root directly, build the dists and release to TestPyPI:

   ```bash
   rm dist/*
   python3 datacommons/setup.py sdist bdist_wheel
   python3 datacommons_pandas/setup.py sdist bdist_wheel
   python3 -m twine upload --repository testpypi dist/*
   ```

## Release to Production PyPI

1. In [datacommons/setup.py](../datacommons/setup.py) and
   [datacommons_pandas/setup.py](../datacommons_pandas/setup.py):

   - Revert the package name to `datacommons` and `datacommons_pandas`
   - Update and double check "VERSION"

1. Update [datacommons/CHANGELOG.md](../datacommons/CHANGELOG.md) and [datacommons_pandas/CHANGELOG.md](../datacommons_pandas/CHANGELOG.md)

1. Build the dists and release to PyPI:

   ```bash
   rm dist/*
   python3 datacommons/setup.py sdist bdist_wheel
   python3 datacommons_pandas/setup.py sdist bdist_wheel
   python3 -m twine upload dist/*
   ```
