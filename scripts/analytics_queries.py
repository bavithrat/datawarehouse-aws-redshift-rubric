import configparser
import psycopg2
from sql_queries import analytics_queries

def analytical_queries(cur, conn):

    print("\nRunning Analytical Queries\n")

    for question, query in analytics_queries:

        print("Question:", question)

        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            print(row)

        print("\n---------------------------------\n")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Run analytics queries
    analytical_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()