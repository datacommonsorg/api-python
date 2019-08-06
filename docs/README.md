# Instructions to Regenerate the Documentation

Documentation for the Data Commons Python Client API is generated using Sphinx
and hosted on readthedocs.org.

## Generating Documentation Locally

To generate documentation locally,

1.  Autogenerate the API documentation using `sphinx-build` by running the
    following command in the root directory of this repository:

    ```
    sphinx-build -a docs/source docs/build
    ```

    This generates documentation as `.rst` files from the Python docstrings of
    externally facing functions, classes and method in the API while ignoring
    test and example files.

2.  Navigate to the `docs/` directory and execute `make html`. This will format
    the `.rst` files into `.html`files.

3.  Open `docs/build/html/index.html` to access documentation.

Local documentation is not meant to be committe! Readthedocs will automatically
build the documentation on a push to the Github repository.

## Additional Details About Documenting Data Commons

- We use the `Sphix.ext.napoleon` extension to parse Google style docstrings.
