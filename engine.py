
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

class Engine:

    def __init__(self, obj):
        self.user = obj["USER"]
        self.pwd = obj["PASSWORD"]
        self.host = obj["HOST"]
        self.service = obj["SERVICE_NAME"]
        self.port = obj["PORT"]
        self.dsn = obj["DSN"]
        self.pool_pre_ping = True
        self.thick_mode = False

class InitializeEngine(Engine):

    def __init__(self):
        pass

    def create(self, dict):
        if dict is not None:
            try:
                engine = sqlalchemy.create_engine(
                f'oracle+oracledb://{dict.user}:{dict.pwd}@{dict.host}:{dict.port}/?service_name={dict.service}',
                thick_mode=dict.thick_mode, pool_pre_ping=dict.pool_pre_ping)

                return engine
            except SQLAlchemyError as e:
                return e
            
        return None


