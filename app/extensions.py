from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_migrate import Migrate
from app.utils import GUID
from celery import Celery

db = SQLAlchemy()
migrate = Migrate()
ma = Marshmallow()
celery = Celery()
db.__setattr__("GUID", GUID)
