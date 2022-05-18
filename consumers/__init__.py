import logging
import os

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database, drop_database


from settings import *
from consumers.models import Base, Message

logger = logging.getLogger('message_parser') 

def create_pg_pool():
    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_db = PG_DB
    pg_user = PG_USER
    pg_passwd = PG_PASSWD

    pg_db_connection = "postgresql://{user}:{passwd}@{host}/{db}".format(
            user=pg_user,
            passwd=pg_passwd,
            host=pg_host,
            db=pg_db
        )
    pg_pool = create_engine(pg_db_connection)
    # TODO: Add args to drop db from cmd.
    # drop_database(pg_db_connection)
    if not database_exists(pg_pool.url):
      create_database(pg_pool.url)

    return pg_pool

init_pg_pool = create_pg_pool()
# Create all tables defined in models.
Base.metadata.create_all(init_pg_pool, checkfirst=True)
init_pg_pool.dispose()