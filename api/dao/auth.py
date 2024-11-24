from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import time
import uuid

from api.exceptions.validation import ValidationException
from api.exceptions.notfound import NotFoundException
from api.db import get_db_session, get_db_transaction

class AuthDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver, jwt_secret):
        self.driver = driver
        self.jwt_secret = jwt_secret

    """
    This method should create a new User node in the database with the email and
    name provided, along with an encrypted version of the password and a UUID.
    """
    def register(self, email, plain_password, name):
        # Generate a UUID
        user_id = str(uuid.uuid4())
        
        # Hash Password - using pbkdf2:sha256 method
        encrypted = generate_password_hash(plain_password, method='pbkdf2:sha256')

        # Create User
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                # Check if a user already exists
                result = tx.run("""
                    MATCH (u:User {email: $email}) RETURN u
                """, email=email)

                if result.single() != None:
                    raise ValidationException("User already exists", {
                        "email": "Email already in use"
                    })

                result = tx.run("""
                    CREATE (u:User {
                        userId: $userId,
                        email: $email,
                        password: $password,
                        name: $name
                    })
                    RETURN u {
                        .userId,
                        .name,
                        .email
                    } AS u
                """, userId=user_id, email=email, password=encrypted, name=name)

                user = result.single().get("u")
                user["token"] = self._generate_token(user)

                return user

    """
    This method should attempt to find a User node with the email and password
    provided, if one is found return the user information along with a JWT token.
    """
    def authenticate(self, email, plain_password):
        with get_db_session(self.driver) as session:
            with get_db_transaction(session) as tx:
                result = tx.run("""
                    MATCH (u:User {email: $email})
                    RETURN u { .userId, .email, .name, .password } AS user
                """, email=email)

                record = result.single()
                if record is None:
                    return False

                user = record.get("user")
                if not user or not check_password_hash(user["password"], plain_password):
                    return False

                payload = {
                    "userId": user["userId"],
                    "email": user["email"],
                    "name": user["name"],
                }
                payload["token"] = self._generate_token(payload)
                return payload

    """
    Generate a JWT token for the user
    """
    def _generate_token(self, payload):
        # Configure JWT
        claims = {
            "sub": payload["userId"],
            "name": payload["name"],
            "email": payload["email"],
            "iat": int(time.time()),
            "exp": int(time.time()) + 2629800 # 1 month in seconds
        }

        return jwt.encode(claims, self.jwt_secret, algorithm='HS256')
