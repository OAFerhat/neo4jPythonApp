from contextlib import contextmanager
from neo4j import Driver

@contextmanager
def get_db_session(driver: Driver):
    """Context manager for Neo4j database sessions.
    
    Usage:
        with get_db_session(driver) as session:
            result = session.execute_read(some_func)
    """
    session = None
    try:
        session = driver.session()
        yield session
    finally:
        if session is not None:
            session.close()

@contextmanager
def get_db_transaction(session, access_mode="READ"):
    """Context manager for Neo4j transactions.
    
    Usage:
        with get_db_transaction(session) as tx:
            result = tx.run("MATCH (n) RETURN n")
    """
    tx = None
    try:
        tx = session.begin_transaction()
        yield tx
        tx.commit()
    except Exception as e:
        if tx is not None:
            tx.rollback()
        raise e
    finally:
        if tx is not None:
            tx.close()
