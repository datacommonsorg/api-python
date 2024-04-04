# Python API Development

The Python API currently supports `python>=3.7`.

To test, run:

```bash
./run_tests.sh -p
```

To debug the continuous integration tests, run:

```bash
gcloud builds submit . --project=datcom-ci --config=cloudbuild.yaml
```

Both commands will run the same set of tests.

To run the examples:

```bash
python -m datacommons.examples.XXX
```

where XXX is the module you want to run.
