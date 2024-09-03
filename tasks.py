import os
import sys
import invoke
from dotenv import load_dotenv

project_root = os.path.dirname(__file__) # Assumes this tasks file is in project root
venv_path = os.path.join(project_root, '.venv')
env_path = os.path.join(project_root, '.env')

load_dotenv(env_path)

# To run all setup tasks: invoke setup-all
# To run individual tasks: invoke <task-name>

@invoke.task
def install_packages(c):
    """Install required packages in the virtual environment."""
    print("Running PIP installs quietly...")
    with c.prefix(f"source {os.path.join(venv_path, 'bin', 'activate')}"):
        c.run("pip install -q -r requirements.txt")

@invoke.task
def create_admin(c):
    """Create admin user for Superset."""
    superset_user = os.environ.get('SUPERSET_USER')
    superset_password = os.environ.get('SUPERSET_PASSWORD')
    if not all([superset_user, superset_password]):
        print("Error: SUPERSET_USER and SUPERSET_PASSWORD must be set.")
        sys.exit(1)
    c.run(f"superset fab create-admin --username {superset_user} --firstname Superset --lastname Admin --email admin@superset.com --password {superset_password}")

@invoke.task
def upgrade_db(c):
    """Create then upgrade Superset database."""
    # c.run(f"sqlite3 {os.environ.get('SUPERSET_META_DATABASE')} \".databases\"")
    c.run("superset db upgrade")

@invoke.task
def setup_roles(c):
    """Setup roles in Superset."""
    c.run("superset init")

@invoke.task
def create_db_connections(c):
    """Create database connections to DuckDB in Superset."""
    database_path = os.environ.get('DATABASE_PATH')
    database_name = os.environ.get('DATABASE_NAME')
    if not all([database_path, database_name]):
        print("Error: DATABASE_PATH and DATABASE_NAME must be set.")
        sys.exit(1)
    c.run(f"superset set_database_uri -d DuckDB-{database_name} -u duckdb:///{database_path}")

@invoke.task
def initialise_dbt(c):
    """Create basic dbt folder structure and skeleton"""
    c.run("dbt init --profile mds-local dbt_models")
    c.run("dbt deps")
    model_folders = ["intermediate", "marts", "staging"]
    for folder in model_folders:
        with c.cd(f"./{os.environ.get('DBT_PROJECT_DIR')}/models"):
            c.run(f"mkdir -p {folder}")
    

@invoke.task(install_packages, upgrade_db, create_admin, setup_roles, create_db_connections, initialise_dbt)
def setup_all(c):
    """Run all setup tasks."""
    c.run(f"mkdir -p {os.environ.get('DLT_PROJECT_DIR')}/logs")
    print("All setup tasks run")

@invoke.task()
def run_superset(c):
    """Starts Superset using a default port"""
    c.run("superset run -p 8088 --with-threads --reload --debugger")

@invoke.task()
def run_pipelines(c):
    """Runs the data load tool pipelines"""
    c.run(f"python {os.environ.get('DLT_PROJECT_DIR')}/sample_pipeline.py")
    c.run(f"dlt pipeline {os.environ.get('DATABASE_NAME')} show")

@invoke.task()
def build_models(c):
    """Compiles and runs dbt models"""
    c.run("dbt debug")
    c.run("dbt seed")
    c.run("dbt compile")
    c.run("dbt run")