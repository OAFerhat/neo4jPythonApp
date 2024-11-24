from api.exceptions.notfound import NotFoundException
from api.db import get_db_session, get_db_transaction

class FavoriteDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be used to
    interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver = driver

    """
    This method should retrieve a list of movies that have an incoming :HAS_FAVORITE
    relationship from a User node with the supplied `userId`.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.

    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """
    # tag::all[]
    def all(self, user_id, sort='title', order='ASC', limit=6, skip=0):
        # Get list of favorites from the database
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (u:User {{userId: $userId}})-[r:HAS_FAVORITE]->(m:Movie)
                    WHERE m.`{sort}` IS NOT NULL
                    RETURN m {{
                        .*,
                        favorite: true
                    }} AS movie
                    ORDER BY m.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(cypher, userId=user_id, skip=skip, limit=limit)
                return [row.get("movie") for row in result]
    # end::all[]

    """
    This method should create a :HAS_FAVORITE relationship between
    the User and Movie ID nodes provided.

    If either the user or movie cannot be found, a NotFoundError should be thrown.
    """
    # tag::add[]
    def add(self, user_id, movie_id):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (u:User {userId: $userId})
                    MATCH (m:Movie {tmdbId: $movieId})
                    MERGE (u)-[r:HAS_FAVORITE]->(m)
                    ON CREATE SET r.createdAt = datetime()
                    RETURN m {
                        .*,
                        favorite: true
                    } AS movie
                """, userId=user_id, movieId=movie_id)

                record = result.single()
                if record is None:
                    raise NotFoundException()

                return record.get("movie")
    # end::add[]

    """
    This method should remove the :HAS_FAVORITE relationship between
    the User and Movie ID nodes provided.

    If either the user, movie or the relationship between them cannot be found,
    a NotFoundError should be thrown.
    """
    # tag::remove[]
    def remove(self, user_id, movie_id):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie {tmdbId: $movieId})
                    DELETE r
                    RETURN m {
                        .*,
                        favorite: false
                    } AS movie
                """, userId=user_id, movieId=movie_id)

                record = result.single()
                if record is None:
                    raise NotFoundException()

                return record.get("movie")
    # end::remove[]
