from flask import Flask, current_app

# tag::import[]
from neo4j import GraphDatabase
# end::import[]

"""
Initiate the Neo4j Driver
"""
# tag::initDriver[]
def init_driver(uri, username, password):
    try:
        # Create a new instance of the Neo4j driver
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Verify connectivity
        driver.verify_connectivity()
        current_app.driver = driver
        return driver
    except Exception as e:
        print(f"Failed to create Neo4j driver: {str(e)}")
        current_app.driver = None
        return None
# end::initDriver[]


"""
Get the instance of the Neo4j Driver created in the `initDriver` function
"""
# tag::getDriver[]
def get_driver():
    return current_app.driver

# end::getDriver[]

"""
If the driver has been instantiated, close it and all remaining open sessions
"""
# tag::closeDriver[]
def close_driver():
    if current_app.driver is not None:
        current_app.driver.close()
        current_app.driver = None
# end::closeDriver[]
