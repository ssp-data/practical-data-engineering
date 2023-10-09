# Real-Estate Pipeline

change directory to pipeline `cd $git/dagster-data-pipeline/src/pipelines/real-estate`

## Install requirements
```
python -m venv ~/.venvs/practical-de
source ~/.venvs/practical-de/bin/activate
pip install -e ".[dev]"
```

run:
```
dagster dev
```

## Automatic Tests
````
tox
```
