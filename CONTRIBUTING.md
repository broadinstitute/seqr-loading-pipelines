# Contributing

## Tests

Tests for Hail 0.1 and Hail 0.2 scripts must be run in different environments.

### Hail 0.1

Currently, no tests in the hail_scripts/v01 directory involve Hail. To run tests:
```shell
python2 -m unittest discover -s hail_scripts/v01 -p "*test*.py"
```

### Hail 0.2

Tests for hail_scripts/v02 require Hail. Instructions for installing Hail can be found
at https://hail.is/docs/0.2/getting_started.html.

To run tests:
```shell
hail -m unittest discover -s hail_scripts/v02 -p "*test*.py"
```
