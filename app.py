import sys

from util import get_tables, load_db_details
from read import read_table
from write import load_table


def main():
    env = sys.argv[1]
    a_tables = sys.argv[2]
    db_details = load_db_details(env)
    tables = get_tables('table_list', a_tables)
    for table_name in tables['table_name']:
        print(f'reading data for {table_name}')
        data, column_names = read_table(db_details, table_name)
        print(f'loading data for {table_name}')
        load_table(db_details, data, column_names, table_name)


if __name__ == '__main__':
    main()
