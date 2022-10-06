# Python API Release

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
