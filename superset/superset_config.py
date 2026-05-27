import os

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"
)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme-superset-secret")

WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
