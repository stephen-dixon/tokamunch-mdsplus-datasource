# tokamunch-mdsplus-datasource

Python datasource plugin for `tokamunch`.

## Installation

```sh
pip install tokamunch-mdsplus-datasource
```

## Configuration

Register the datasource in your `tokamunch` configuration:

```toml
[data_sources.tokamunch_mdsplus_datasource]
plugin = "tokamunch_mdsplus_datasource"
```

## Development

```sh
python -m pip install -e .[test]
pytest
```
