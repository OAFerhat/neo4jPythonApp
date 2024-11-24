def all(self, sort, order, limit=6, skip=0, user_id=None):
    def get_movies(tx, sort, order, limit, skip, user_id):
        cypher = """
            MATCH (m:Movie)
            WHERE m.`{0}` IS NOT NULL
            RETURN m {{ .* }} AS movie
            ORDER BY m.`{0}` {1}
            SKIP $skip
            LIMIT $limit
        """.format(sort, order)
        result = tx.run(cypher, limit=limit, skip=skip, user_id=user_id)
        return [row.value("movie") for row in result]

    with self.driver.session() as session:
        return session.execute_read(get_movies, sort, order, limit, skip, user_id)



def authenticate(self, email, plain_password):
    def create_user(tx, email, encrypted, name):
        return tx.run("""
            CREATE (u:User {
                userId: randomUuid(),
                email: $email,
                password: $encrypted,
                name: $name
            })
            RETURN u
        """,
        email=email, encrypted=encrypted, name=name
        ).single() 
    with self.driver.session() as session:
        result = session.execute_write(create_user, email, encrypted, name)
        return result

    user = result['u']

    payload = {
        "userId": user["userId"],
        "email":  user["email"],
        "name":  user["name"],
    }

    payload["token"] = self._generate_token(payload)

    return payload

def register(self, email, plain_password, name):
    encrypted = bcrypt.hashpw(plain_password.encode("utf8"), bcrypt.gensalt()).decode('utf8')

    def create_user(tx, email, encrypted, name):
        return tx.run(""" // (1)
            CREATE (u:User {
                userId: randomUuid(),
                email: $email,
                password: $encrypted,
                name: $name
            })
            RETURN u
        """,
        email=email, encrypted=encrypted, name=name # (2)
        ).single() # (3)

    try:
        with self.driver.session() as session:
            result = session.execute_write(create_user, email, encrypted, name)

            user = result['u']

            payload = {
                "userId": user["userId"],
                "email":  user["email"],
                "name":  user["name"],
            }

            payload["token"] = self._generate_token(payload)

        return payload
    except ConstraintError as err:
        # Pass error details through to a ValidationException
        raise ValidationException(err.message, {
            "email": err.message
        })
    