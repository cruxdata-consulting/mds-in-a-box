import os
from dotenv import load_dotenv

load_dotenv('../.env')

SECRET_KEY=os.environ.get('SUPERSET_SECRET_KEY')
# SQLALCHEMY_DATABASE_URI=f"sqlite:///{os.environ.get('SUPERSET_META_DATABASE')}?check_same_thread=False"