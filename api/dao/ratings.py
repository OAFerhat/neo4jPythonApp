from api.exceptions.notfound import NotFoundException
from api.db import get_db_session, get_db_transaction

class RatingDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be used to
    interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver = driver

    """
    Add a relationship between a User and Movie with a `rating` property.
    The `rating` parameter should be converted to a Neo4j Integer.
    """
    # tag::add[]
    def add_rating(self, user_id, movie_id, rating):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                MATCH (u:User {userId: $user_id})
                MATCH (m:Movie {tmdbId: $movie_id})
                MERGE (u)-[r:RATED]->(m)
                SET r.rating = $rating,
                    r.timestamp = timestamp()
                RETURN m {
                    .*,
                    rating: r.rating
                } AS movie
                """, user_id=user_id, movie_id=movie_id, rating=rating)

                record = result.single()
                if record is None:
                    raise NotFoundException()

                return record.get("movie")
    # end::add[]

    """
    Return a paginated list of reviews for a Movie.
    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """
    # tag::forMovie[]
    def for_movie(self, id, sort='timestamp', order='ASC', limit=6, skip=0):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                MATCH (u:User)-[r:RATED]->(m:Movie {{tmdbId: $id}})
                WHERE r.`{sort}` IS NOT NULL
                RETURN r {{
                    .rating,
                    .timestamp,
                    user: u {{
                        .userId, .name
                    }}
                }} AS rating
                ORDER BY r.`{sort}` {order_direction}
                SKIP $skip
                LIMIT $limit
                """

                result = tx.run(cypher, id=id, skip=skip, limit=limit)
                return [row.get("rating") for row in result]
    # end::forMovie[]
