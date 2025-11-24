import argparse
import psycopg2

def populate_database(db_name):
    
    # connect to the database
    conn = psycopg2.connect(f"dbname={db_name}")
    cur = conn.cursor()

    # TODO -> need to populate in order of dependency
    # populate_series


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--db_name", type=str, required=True)

    # eventually want to add argument parsing
    # parser.add_argument("--password", type=str required=False)
    args = parser.parse_args()

    populate_database(args.db_name)


if __name__ == "__main__":
    main()
