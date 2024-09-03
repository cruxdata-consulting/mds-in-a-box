Basic operations
- set up venv
```
python3 -m venv .venv
source .venv/bin/activate
```
- run install scripts
```
pip install invoke python-dotenv
invoke setup-all
```
- run pipeline
```
invoke run-pipelines
```
- run dbt 
```
invoke build-models
```
- open superset
```
invoke run-superset
```