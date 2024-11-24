from api.data import popular, goodfellas
from api.exceptions.notfound import NotFoundException
from api.data import popular
from api.db import get_db_session, get_db_transaction

class MovieDAO:
    def __init__(self, driver):
        self.driver = driver

    def get_user_favorites(self, session, user_id):
        if user_id is None:
            return []

        with get_db_transaction(session) as tx:
            result = tx.run("""
                MATCH (u:User {userId: $userId})-[:HAS_FAVORITE]->(m:Movie)
                RETURN collect(m.tmdbId) AS favorites
            """, userId=user_id)

            record = result.single()
            return record["favorites"] if record else []

    def all(self, sort, order, limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            # Get user favorites first
            favorites = self.get_user_favorites(session, user_id)
            
            # Execute main query
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (m:Movie)
                    WHERE m.`{sort}` IS NOT NULL
                    RETURN m {{
                        .*,
                        favorite: m.tmdbId IN $favorites
                    }} AS movie
                    ORDER BY m.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(
                    cypher,
                    favorites=favorites,
                    skip=skip,
                    limit=limit
                )

                return [row.get("movie") for row in result]

    def get_by_genre(self, name, sort='title', order='ASC', limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            # Get user favorites first
            favorites = self.get_user_favorites(session, user_id)
            
            # Execute main query
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (m:Movie)-[:IN_GENRE]->(g:Genre {{name: $name}})
                    WHERE m.`{sort}` IS NOT NULL
                    RETURN m {{
                        .*,
                        favorite: m.tmdbId IN $favorites
                    }} AS movie
                    ORDER BY m.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(
                    cypher,
                    name=name,
                    favorites=favorites,
                    skip=skip,
                    limit=limit
                )

                return [row.get("movie") for row in result]

    def get_for_actor(self, id, sort='title', order='ASC', limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            favorites = self.get_user_favorites(session, user_id)
            
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (:Person {{tmdbId: $id}})-[:ACTED_IN]->(m:Movie)
                    WHERE m.`{sort}` IS NOT NULL
                    RETURN m {{
                        .*,
                        favorite: m.tmdbId IN $favorites
                    }} AS movie
                    ORDER BY m.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(cypher, id=id, limit=limit, skip=skip, favorites=favorites)
                return [row.get("movie") for row in result]

    def get_for_director(self, id, sort='title', order='ASC', limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            favorites = self.get_user_favorites(session, user_id)
            
            with get_db_transaction(session) as tx:
                order_direction = "DESC" if order.upper() == "DESC" else "ASC"
                cypher = f"""
                    MATCH (:Person {{tmdbId: $id}})-[:DIRECTED]->(m:Movie)
                    WHERE m.`{sort}` IS NOT NULL
                    RETURN m {{
                        .*,
                        favorite: m.tmdbId IN $favorites
                    }} AS movie
                    ORDER BY m.`{sort}` {order_direction}
                    SKIP $skip
                    LIMIT $limit
                """

                result = tx.run(cypher, id=id, limit=limit, skip=skip, favorites=favorites)
                return [row.get("movie") for row in result]

    def find_by_id(self, id, user_id=None):
        with get_db_session(self.driver) as session:
            favorites = self.get_user_favorites(session, user_id)
            
            with get_db_transaction(session) as tx:
                cypher = """
                    MATCH (m:Movie {tmdbId: $id})
                    OPTIONAL MATCH (m)<-[r:ACTED_IN]-(a:Person)
                    OPTIONAL MATCH (m)<-[d:DIRECTED]-(dir:Person)
                    OPTIONAL MATCH (m)-[g:IN_GENRE]->(genre:Genre)
                    OPTIONAL MATCH (m)<-[rated:RATED]-()
                    WITH m, 
                         collect(DISTINCT { 
                            name: a.name,
                            id: a.tmdbId,
                            poster: a.poster,
                            roles: r.roles
                         }) AS actors,
                         collect(DISTINCT {
                            name: dir.name,
                            id: dir.tmdbId,
                            poster: dir.poster
                         }) AS directors,
                         collect(DISTINCT genre.name) AS genres,
                         count(rated) AS ratingCount
                    RETURN m {
                        .*,
                        actors: actors,
                        directors: directors,
                        genres: genres,
                        ratingCount: ratingCount,
                        favorite: m.tmdbId IN $favorites
                    } AS movie
                """

                result = tx.run(
                    cypher,
                    id=id,
                    favorites=favorites
                )
                
                record = result.single()
                if record is None:
                    raise NotFoundException()

                return record.get("movie")

    def get_similar_movies(self, id, limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            favorites = self.get_user_favorites(session, user_id)

            with get_db_transaction(session) as tx:
                cypher = """
                MATCH (:Movie {tmdbId: $id})-[:IN_GENRE|ACTED_IN|DIRECTED]->()<-[:IN_GENRE|ACTED_IN|DIRECTED]-(m)
                WHERE m.imdbRating IS NOT NULL
                WITH m, count(*) AS inCommon
                WITH m, inCommon, m.imdbRating * inCommon AS score
                ORDER BY score DESC
                SKIP $skip
                LIMIT $limit
                RETURN m {
                    .*,
                    score: score,
                    favorite: m.tmdbId IN $favorites
                } AS movie
                """

                result = tx.run(cypher, id=id, limit=limit, skip=skip, favorites=favorites)
                return [row.get("movie") for row in result]
