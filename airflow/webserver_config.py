import os
from airflow.www.fab_security.manager import AUTH_DB

# The secret key is used by Flask to encrypt session cookies.
#SECRET_KEY = 'your_secret_key_here'

# The SQLAlchemy connection string to your metadata database.
SQLALCHEMY_DATABASE_URI = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///airflow.db")

# The authentication type
AUTH_TYPE = AUTH_DB

# Theme
DEFAULT_THEME = "default"
