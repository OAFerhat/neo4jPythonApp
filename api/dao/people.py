from api.data import people, pacino
from api.exceptions.notfound import NotFoundException
from api.db import get_db_session, get_db_transaction


class PeopleDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a paginated list of People (actors or directors),
    with an optional filter on the person's name based on the `q` parameter.

    Results should be ordered by the `sort` parameter and limited to the
    number passed as `limit`.  The `skip` variable should be used to skip a
    certain number of rows.
    """
    # tag::all[]
    def all(self, q, sort='name', order='ASC', limit=6, skip=0):
        # Get a list of people from the database
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (p:Person)
                    WHERE p.`{sort}` IS NOT NULL
                    {" AND p.name CONTAINS $q" if q else ""}
                    RETURN p {{
                        .*,
                        actedCount: count{{ (p)-[:ACTED_IN]->() }},
                        directedCount: count{{ (p)-[:DIRECTED]->() }}
                    }} AS person
                    ORDER BY p.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(cypher, q=q, skip=skip, limit=limit)

                return [row.get("person") for row in result]
    # end::all[]

    """
    Find a user by their ID.

    If no user is found, a NotFoundError should be thrown.
    """
    # tag::findById[]
    def find_by_id(self, id):
        # Find a user by their ID
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (p:Person {tmdbId: $id})
                    RETURN p {
                        .*,
                        actedCount: count{ (p)-[:ACTED_IN]->() },
                        directedCount: count{ (p)-[:DIRECTED]->() }
                    } AS person
                """, id=id)

                record = result.single()
                if record is None:
                    raise NotFoundException()

                return record.get("person")
    # end::findById[]

    """
    Get a list of similar people to a Person, ordered by their similarity score
    in descending order.
    """
    # tag::getSimilarPeople[]
    def get_similar_people(self, id, limit=6, skip=0):
        # Get a list of similar people to the person by their id
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (:Person {tmdbId: $id})-[:ACTED_IN|DIRECTED]->(m)<-[r:ACTED_IN|DIRECTED]-(p)
                    WITH p, collect(m {.tmdbId, .title, type: type(r)}) as movies
                    WITH p, movies, 
                         count{ (p)-[:ACTED_IN]->() } as acted,
                         count{ (p)-[:DIRECTED]->() } as directed
                    RETURN p {
                        .*,
                        actedCount: acted,
                        directedCount: directed,
                        inCommon: movies
                    } AS person
                    ORDER BY size(person.inCommon) DESC
                    SKIP $skip
                    LIMIT $limit
                """, id=id, skip=skip, limit=limit)

                return [row.get("person") for row in result]
    # end::getSimilarPeople[]