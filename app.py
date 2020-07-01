import sys
from loguru import logger

from util import get_tables, load_db_details
from read import read_table
from write import load_table


def main():
    env = sys.argv[1]
    a_tables = sys.argv[2]
    logger.add("data-copier.info",
               rotation="1 MB",
               retention="10 days",
               level="INFO"
               )
    logger.add("data-copier.err",
               rotation="1 MB",
               retention="10 days",
               level="ERROR"
               )
    db_details = load_db_details(env)
    tables = get_tables('table_list', a_tables)
    for table_name in tables['table_name']:
        logger.info(f'reading data for {table_name}')
        data, column_names = read_table(db_details, table_name)
        logger.info(f'loading data for {table_name}')
        load_table(db_details, data, column_names, table_name)


def truncate_tables(env, tables):
    db_details = load_db_details(env)



if __name__ == '__main__':
    main()
