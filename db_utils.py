import pandas as pd
from sqlalchemy import create_engine

def connectToPostgres(host, port, database, user, password):
    """
    Attempts to establish a connection to a PostgreSQL database using SQLAlchemy.

    Args:
        host (str): The hostname or IP address of the database server.
        port (int): The port number on which the database server is listening.
        database (str): The name of the database to connect to.
        user (str): The username to use for authentication.
        password (str): The password associated with the specified username.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object if successful, None otherwise.
        error_message (str): An error message if the connection fails, None otherwise.
    """

    try:
        # Construct the database connection URL
        connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create the SQLAlchemy engine
        engine = create_engine(connection_url)

        # Test the connection by attempting to connect
        with engine.connect() as conn:
            print("Connection to PostgreSQL database successful!")
            return engine, None  # Return engine object and no error message

    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None, str(e)  # Return no engine and the error message

def getDataFromDB(con, q):
    df = pd.read_sql_query(q, con)
    return(df)