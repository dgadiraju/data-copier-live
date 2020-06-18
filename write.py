from util import get_connection


def build_insert_query(table_name, column_names):
    column_values = tuple(map(lambda column: column.replace(column, '%s')))
    query = (f'''
        INSERT INTO {table_name} {column_names} VALUES {column_values}
    ''')
    return query


def insert_data(connection, cursor, query, data, batch_size=100):
    return
