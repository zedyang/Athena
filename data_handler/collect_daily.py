
from Athena.data_handler.sql_wrapper import SQLWrapper


if __name__ == '__main__':
    d = SQLWrapper()
    d.migrate_data()