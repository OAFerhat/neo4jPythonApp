from flask import Flask, current_app
import atexit
import threading
from contextlib import contextmanager

# tag::import[]
from neo4j import GraphDatabase
# end::import[]

# Store the driver instance in a thread-local storage
_driver = threading.local()

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
        
        # Store in both thread-local and app context
        _driver.instance = driver
        if current_app:
            current_app.driver = driver
        
        # Register cleanup on exit
        atexit.register(close_driver)
        
        return driver
    except Exception as e:
        print(f"Failed to create Neo4j driver: {str(e)}")
        return None
# end::initDriver[]

"""
Get the instance of the Neo4j Driver created in the `initDriver` function
"""
# tag::getDriver[]
def get_driver():
    # Try to get from thread-local first
    if hasattr(_driver, 'instance'):
        return _driver.instance
    # Fallback to app context
    return current_app.driver if current_app else None
# end::getDriver[]

"""
Close the Neo4j Driver
"""
# tag::closeDriver[]
def close_driver():
    # Close driver from thread-local storage
    if hasattr(_driver, 'instance') and _driver.instance is not None:
        _driver.instance.close()
        _driver.instance = None
    
    # Also close from app context if it exists
    try:
        if current_app and hasattr(current_app, 'driver') and current_app.driver is not None:
            current_app.driver.close()
            current_app.driver = None
    except RuntimeError:
        # Ignore runtime errors when there's no application context
        pass
# end::closeDriver[]

@contextmanager
def driver_context(uri=None, username=None, password=None):
    """Context manager for Neo4j driver to ensure proper cleanup.
    
    Usage:
        with driver_context(uri, username, password) as driver:
            # use driver here
    """
    driver = None
    try:
        if uri and username and password:
            driver = init_driver(uri, username, password)
        else:
            driver = get_driver()
        if driver is None:
            raise Exception("Failed to initialize Neo4j driver")
        yield driver
    finally:
        if driver is not None:
            close_driver()
