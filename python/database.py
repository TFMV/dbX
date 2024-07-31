import adbc_driver_postgresql.dbapi as pgdbapi
import adbc_driver_sqlite.dbapi as sqldbapi
import yaml

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

def get_pg_uri(db_config):
    return f"postgresql://{db_config['user']}:{db_config['password']}@" \
           f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

class DatabaseConnectionManager:
    def __init__(self):
        self.source_conn = self.connect_to_database(config['database']['source'])
        self.target_conn = self.connect_to_database(config['database']['target'])

    def connect_to_database(self, db_config):
        if db_config['type'] == 'postgres':
            return pgdbapi.connect(get_pg_uri(db_config))
        elif db_config['type'] == 'sqlite':
            return sqldbapi.connect(db_config['dbname'])
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")

    def get_connection(self, db_role: str):
        if db_role == "source":
            return self.source_conn
        elif db_role == "target":
            return self.target_conn
        else:
            raise ValueError("Invalid database role. Use 'source' or 'target'.")

db_manager = DatabaseConnectionManager()
