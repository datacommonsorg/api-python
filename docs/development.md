# Python API Development

This client library supports `python>=3.10`.

## Set up
If you haven't already, clone this repository.

```bash
git clone https://github.com/datacommonsorg/api-python.git
cd api-python
```

To set up the Python environment for development, run:

```bash
./run_test.sh -s
```

This will install `hatch`, which is the main tool used to manage the
environment, dependencies, and development tools. You can also manually install
`hatch` and create a virtual environment.

```bash
pip install hatch
hatch env create
```

## Code style and linting
We use `isort` and `yapf` for code formatting. Check formatting with:

```bash
hatch run lint:check
```

To automatically fix formatting run:

```bash
hatch run lint:format
```

## Running tests

To test, run:

```bash
hatch run test:all
```

To debug the continuous integration tests, run:

```bash
gcloud builds submit . --project=datcom-ci --config=cloudbuild.yaml
```

Both commands will run the same set of tests.