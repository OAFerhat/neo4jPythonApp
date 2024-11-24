import os
import pytest

from api.neo4j import get_driver, driver_context
from api.dao.ratings import RatingDAO

movie = '769'
user = '1185150b-9e81-46a2-a1d3-eb649544b9c4'
email = 'graphacademy.reviewer@neo4j.com'
rating = 5

@pytest.fixture(autouse=True)
def before_all(app):
    with app.app_context():
        uri = app.config.get('NEO4J_URI')
        username = app.config.get('NEO4J_USERNAME')
        password = app.config.get('NEO4J_PASSWORD')
        
        with driver_context(uri, username, password) as driver:
            with driver.session() as session:
                session.execute_write(lambda tx: tx.run("""
                    MERGE (u:User {userId: $user})
                    SET u.email = $email
                    MERGE (m:Movie {tmdbId: $movie})
                """, user=user, movie=movie, email=email).consume())

def test_store_integer(app):
    with app.app_context():
        uri = app.config.get('NEO4J_URI')
        username = app.config.get('NEO4J_USERNAME')
        password = app.config.get('NEO4J_PASSWORD')
        
        with driver_context(uri, username, password) as driver:
            dao = RatingDAO(driver)
            output = dao.add_rating(user, movie, rating)
            assert output["tmdbId"] == movie
            assert output["rating"] is rating
