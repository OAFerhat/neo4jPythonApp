import os
import tempfile

import pytest
from dotenv import load_dotenv

from api import create_app
from api.neo4j import driver_context

@pytest.fixture(scope='session', autouse=True)
def load_env():
    load_dotenv()

@pytest.fixture
def app():
    app = create_app({'TESTING': True})
    return app

@pytest.fixture
def client(app):
    with driver_context(
        os.environ.get('NEO4J_URI'),
        os.environ.get('NEO4J_USERNAME'),
        os.environ.get('NEO4J_PASSWORD'),
    ) as driver:
        with app.test_client() as client:
            with app.app_context():
                yield client
