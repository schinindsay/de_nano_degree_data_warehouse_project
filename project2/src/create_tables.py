import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
drops tables
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(f"Running this drop table query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(f"Running this create table query: {query}")
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
#     KEY=config.get('AWS','key')
#     SECRET= config.get('AWS','secret')
#     print('key ', KEY)
#     print('secret ', SECRET)
    
    print(*config['CLUSTER'].values())


    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
if __name__ == "__main__":
    main()