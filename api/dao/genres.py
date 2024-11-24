from api.exceptions.notfound import NotFoundException
from api.db import get_db_session, get_db_transaction

class GenreDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a list of genres from the database with a
    `name` property, `movies` which is the count of the incoming `IN_GENRE`
    relationships and a `poster` property to be used as a background.

    [
       {
        name: 'Action',
        movies: 1545,
        poster: 'https://image.tmdb.org/t/p/w440_and_h660_face/qJ2tW6WMUDux911r6m7haRef0WH.jpg'
       }, ...

    ]
    """
    # tag::all[]
    def all(self):
        # Define a unit of work to Get a list of Genres
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (g:Genre)
                    WHERE g.name <> '(no genres listed)'
                    CALL {
                        WITH g
                        MATCH (g)<-[:IN_GENRE]-(m:Movie)
                        WHERE m.imdbRating IS NOT NULL AND m.poster IS NOT NULL
                        RETURN m.poster AS poster
                        ORDER BY m.imdbRating DESC LIMIT 1
                    }
                    RETURN g {
                        .*,
                        movies: count { (g)<-[:IN_GENRE]-(:Movie) },
                        poster: poster
                    } AS genre
                    ORDER BY g.name ASC
                """)

                return [row.get("genre") for row in result]
    # end::all[]


    """
    This method should find a Genre node by its name and return a set of properties
    along with a `poster` image and `movies` count.

    If the genre is not found, a NotFoundError should be thrown.
    """
    # tag::find[]
    def find(self, name):
        # Define a unit of work to find the genre by it's name
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (g:Genre {name: $name})<-[:IN_GENRE]-(m:Movie)
                    WHERE m.imdbRating IS NOT NULL AND m.poster IS NOT NULL AND g.name <> '(no genres listed)'
                    WITH g, m
                    ORDER BY m.imdbRating DESC
                    WITH g, head(collect(m)) AS movie
                    RETURN g {
                        .name,
                        movies: count { (g)<-[:IN_GENRE]-() },
                        poster: movie.poster
                    } AS genre
                """, name=name)

                record = result.single()
                # If no records are found raise a NotFoundException
                if record is None:
                    raise NotFoundException()

                return record.get("genre")
    # end::find[]

    def get_by_genre(self, name, sort='title', order='ASC', limit=6, skip=0, user_id=None):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                # Get user favorites first
                favorites = []
                if user_id is not None:
                    fav_result = tx.run("""
                        MATCH (u:User {userId: $userId})-[:HAS_FAVORITE]->(m)
                        RETURN collect(m.tmdbId) as favorites
                    """, userId=user_id)
                    favorites = fav_result.single()["favorites"]

                # Execute main query
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