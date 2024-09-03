import os
import yaml
import re
import difflib
from typing import Dict, Any
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from dlt.common.pipeline import LoadInfo
from dlt.common import logger

class DbtModelGenerator:
    def __init__(self, dbt_staging_dir: str, source_name: str, config_file: str = None):
        self.config = self._load_config(config_file)
        self.source_name = source_name
        self._jinja_env = Environment(loader=FileSystemLoader(os.environ.get('DLT_PROJECT_DIR')))
        self._dbt_staging_dir = dbt_staging_dir
        self._model_dir = f"{dbt_staging_dir}/{source_name}"
        self._current_table = {}

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        default_config = {
            'default_materialization': 'view',
            'schema_prefix': 'stg_',
            'column_renaming': [],
            'column_transformations': [],
            'post_hook_sql': '',
            'breaking_changes': ['remove_column'],
            'table_specific': {},
            'dbt_config': {}
        }

        if config_file:
            try:
                with open(config_file, 'r') as f:
                    user_config = yaml.safe_load(f)
                
                # Merge user config with default config
                for key, value in user_config.items():
                    if isinstance(value, dict) and key in default_config:
                        default_config[key].update(value)
                    else:
                        default_config[key] = value
            except FileNotFoundError:
                print(f"Warning: Config file {config_file} not found. Using default configuration.")
            except yaml.YAMLError as e:
                print(f"Error parsing config file: {e}")
                print("Using default configuration.")

        return default_config

    def generate_models(self, load_info: LoadInfo):
        # @TODO check when the loadpackages array has more than one item
        pipeline_schema = load_info.load_packages[0].schema
        for table_name, table_schema in pipeline_schema.tables.items():
            if not re.match(self.config.get('ignore_tables'), table_name):
                self._generate_or_update_dbt_definitions(table_name, table_schema)

    def _generate_or_update_dbt_definitions(self, table_name: str, table_schema: Dict[str, Any]) -> None:
        self._current_table['name'] = table_name
        self._current_table['model_dir'] = f"{self._dbt_staging_dir}/{self.source_name}"
        self._current_table['source_schema_file'] = f"{self._current_table['model_dir']}/_source_schema.yml"
        self._current_table['model_file'] = f"{self._current_table['model_dir']}/{self.config.get('schema_prefix')}{table_schema.get('name')}.sql"
        self._current_table['schema_file'] = f"{self._current_table['model_dir']}/{self.config.get('schema_prefix')}{table_schema.get('name')}.yml"

        # Generate new model content
        for k in table_schema.get('columns').keys():
            for policy in self.config.get('column_renaming'):
                # load the regex scripts and apply migrations, then save back into the table schema dict
                if any([re.match(fr"{match_policy}", k) for match_policy in policy.get('regex_match')]):
                    renamed = re.sub(fr"{policy.get('regex_match')}", fr"{policy.get('regex_replace')}", k)
                else:
                    renamed = k
                table_schema['columns'][k]['name_renamed'] = renamed
        
        new_model_content = self._render_sql_template(table_schema)
        new_source_schema = self._update_schema_yml(table_name)
        new_table_schema = self._update_table_schema_yml(table_schema)

        if not os.path.exists(self._current_table['model_dir']):
            os.makedirs(self._current_table['model_dir'])

        if any([os.path.exists(self._current_table['schema_file']), os.path.exists(self._current_table['model_file'])]):
            self._backup_if_needed(new_model_content, new_table_schema)
        
        # Write or update the files
        with open(self._current_table['model_file'], 'w') as f:
            f.write(new_model_content)
        
        with open(self._current_table['schema_file'], 'w') as f:
            yaml.dump(new_table_schema, f, sort_keys=False)
        
        with open(self._current_table['source_schema_file'], 'w') as f:
            yaml.dump(new_source_schema, f, sort_keys=False)

        return

    def _backup_if_needed(self, new_model_content: str, new_table_schema: Dict[str, Any]) -> None:
        # if there is an existing schema file, we use that as the source of truth for cols and types
        # if there is an existing model, we do a diff between the new and old jinja template, then back up the sql in case there are custom edits
        if not any([os.path.exists(self._current_table['schema_file']), os.path.exists(self._current_table['model_file'])]):
            return  # No existing model to backup
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            with open(self._current_table['schema_file'], 'r') as f:
                existing_table_schema = yaml.safe_load(f)

            if self._has_breaking_changes(existing_table_schema, new_table_schema):
                backup_schema_file = os.path.join(self._model_dir, f"{self.config.get('schema_prefix')}{self._current_table['name']}_{timestamp}.yml.bkup")
                with open(backup_schema_file, 'w') as f:
                    f.write(existing_table_schema)
                logger.warn(f"Breaking changes detected. Backup created: {backup_schema_file}")
        except FileNotFoundError:
            logger.info("No existing schema to back up")
        
        try:
            with open(self._current_table['model_file'], 'r') as f:
                existing_model_content = f.read()
            diff = difflib.SequenceMatcher(a=re.sub(r'\s+', '', existing_model_content), b=re.sub(r'\s+', '', new_model_content))
            if diff.ratio() == 1:
                logger.info(f"Staging models for {self._current_table['name']} are identical")
            else:
                backup_model_file = os.path.join(self._model_dir, f"{self.config.get('schema_prefix')}{self._current_table['name']}_{timestamp}.sql.bkup")
                with open(backup_model_file, 'w') as f:
                    f.write(existing_model_content)
                logger.warn(f"Staging model changes detected. Backup created: {backup_model_file}")
        except FileNotFoundError:
            logger.info('No existing model to back up')
        
        return

    def _has_breaking_changes(self, existing_table_schema: Dict[str, Any], new_table_schema: Dict[str, Any]) -> bool:
        # @TODO investigate replacing this approach with model contracts in dbt
        breaking_changes = False
        existing_cols = {col['name']: col.get('tests') for col in existing_table_schema['models'][0]['columns']}
        new_cols = {col['name']: col.get('tests') for col in new_table_schema['models'][0]['columns']}
        # read the config. For each item in the config, run a series of tests
        for test in self.config['breaking_changes']:
            if test == 'remove_column':
                removed_columns = set(existing_cols.keys()) - set(new_cols.keys())
                if len(removed_columns) > 0:
                    breaking_changes = True
            if test == 'change_column_type':
                # @TODO col types not stored in staging layer ymls yet
                pass
            if test == 'change_primary_key':
                existing_unique_cols = [k for k,v in existing_cols.items() if (not v is None) and (set(v) & set(['unique']))]
                new_unique_cols = [k for k,v in new_cols.items() if (not v is None) and (set(v) & set(['unique']))]
                if not existing_unique_cols == new_unique_cols:
                    breaking_changes = True
        return breaking_changes

    def _render_sql_template(self, table_schema: Dict[str, Any]) -> str:
        template = self._jinja_env.get_template('staging_model.sql.j2')
        columns = []
        for k,v in table_schema.get('columns').items():
            columns.append({
                'name_raw': v['name'],
                'name_renamed': v['name_renamed']
            })
        return template.render(
            source_name=self.source_name,
            table_name=table_schema.get('name'),
            columns=columns,
            config=self.config
        )

    def _update_schema_yml(self, table_name: str) -> Dict[str, Any]:
        try:
            with open(self._current_table['source_schema_file'], 'r') as f:
                sources_yml = yaml.safe_load(f)
        except:
            sources_yml = {'version': 2, 'sources': []}
        
        # Find if the schema already exists in sources
        existing_source = next((source for source in sources_yml['sources'] 
                                if source['name'] == self.source_name), None)
        
        if existing_source:
            # Update existing schema
            existing_source['schema'] = self.source_name
            existing_tables = set(table['name'] for table in existing_source['tables'])
            if table_name not in existing_tables:
                existing_source['tables'].append({'name': table_name})
        else:
            # Add new schema and table
            new_schema = {
                'name': self.source_name,
                'schema': self.source_name,
                'tables': [{'name': table_name}]
            }
            sources_yml['sources'].append(new_schema)

        return sources_yml

    def _update_table_schema_yml(self, table_schema: Dict[str, Any]) -> Dict[str, Any]:
        output_columns = []

        for column_name, properties in table_schema['columns'].items():
            column_info = {'name': column_name}

            if description := properties.get('description'):
                column_info['description'] = description
            
            tests = []
            if properties.get('nullable') is False:
                tests.append('not_null')
            if properties.get('unique') is True:
                tests.append('unique')
            
            if tests:
                column_info['tests'] = tests
            
            output_columns.append(column_info)

        schema_content = {'version': 2, 'models': []}
        
        table_info = {}
        table_info['name'] = f"{self.config.get('schema_prefix')}{table_schema.get('name')}"
        if description := table_schema.get('description'):
            table_info['description'] = description
        table_info['columns'] = output_columns
        
        schema_content['models'].append(table_info)

        return schema_content
