import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@contextlib.contextmanager
def get_db(_type='db'):
    _db = None
    try:
        cnx_str = 'mysql+mysqlconnector://root:@localhost/reseptit'
        if _type == 'dw':
            cnx_str = 'mysql+mysqlconnector://root:@localhost/resepti_olap'
        engine = create_engine(cnx_str)
        db_session = sessionmaker(bind=engine)

        _db = db_session()
        yield _db
    except Exception as e:
        print(e)
    finally:
        if _db is not None:
            _db.close()