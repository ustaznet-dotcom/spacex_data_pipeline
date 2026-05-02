import os

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@postgres/superset_metadata"
)

SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'CHANGE_ME')
