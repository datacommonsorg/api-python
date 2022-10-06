# Python API Release

Note: Always release `datacommons_pandas` when `datacommons` is released.

**If this is your first time releasing to PyPI**, please review the PyPI guide
starting from the
[setup section](https://packaging.python.org/tutorials/packaging-projects/#creating-setup-py).

## Release to Test PyPI

1. In [datacommons/setup.py](../datacommons/setup.py) and [datacommons_pandas/setup.py](../datacommons_pandas/setup.py):

   - Append "-USERNAME" to the package "NAME". For example,
     `NAME = 'foo_package-janedoe123'`.
   - Increment the "VERSION" codes to something that has not been used in your
     test project. This will not affect the production PyPI versioning.

1. Build the dists:

   ```bash
   rm dist/*
   python3 -m pip install --user --upgrade setuptools wheel
   python3 datacommons/setup.py sdist bdist_wheel
   python3 datacommons_pandas/setup.py sdist bdist_wheel
   ```

1. Release the dists to TestPyPI:

   ```bash
   python3 -m pip install --user --upgrade twine
   python3 -m twine upload --repository testpypi dist/*
   ```

## Release to Production PyPI

1. In [datacommons/setup.py](../datacommons/setup.py) and
   [datacommons_pandas/setup.py](../datacommons_pandas/setup.py):

   - Revert the package name to `datacommons` and `datacommons_pandas`
   - Update and double check "VERSION"

2. Update [datacommons/CHANGELOG.md](../datacommons/CHANGELOG.md) and [datacommons_pandas/CHANGELOG.md](../datacommons_pandas/CHANGELOG.md)

3. Build the dists:

   ```bash
   rm dist/*
   python3 -m pip install --user --upgrade setuptools wheel
   python3 datacommons/setup.py sdist bdist_wheel
   python3 datacommons_pandas/setup.py sdist bdist_wheel
   ```

4. Release the dists to PyPI:

   ```bash
   python3 -m pip install --user --upgrade twine
   twine upload dist/*
   ```
