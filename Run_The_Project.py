import psycopg2
from server_interface import TripleStore
from postgres_triple_store import \
    MySQLTripleStore  # Assuming your PostgresTripleStore class is defined in a separate module
import subprocess
from datetime import datetime
from server_interface import TripleStore
from MongDB_store import MongoDBTripleStore

import mysql.connector

def start_mongodb_server():
    # Start MongoDB server using subprocess
    print("Starting MongoDB server...")
    subprocess.Popen(["mongod"])  # Adjust the command as per your MongoDB installation

def main():
    # Create MongoDB and MySQL triple stores
    mongodb_triple_store = MongoDBTripleStore(dbname="triples", host="localhost", port=27017)
    dbname = "triples"
    user = "root"
    password = "Shishir@123"
    host = "localhost"
    port = 3306
    triple_store = MySQLTripleStore(dbname, user, password, host, port)

    #loading the tsvs
    mongodb_triple_store.load_tsv_file(r"C:\Users\shahi\OneDrive\Documents\data.txt")
    triple_store.load_tsv_file(r"C:\Users\shahi\OneDrive\Documents\data.txt")
    # Interactive terminal session
    while True:
        print("Choose a server to interact with:")
        print("1. MongoDB")
        print("2. MySQL")
        print("3. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            server = mongodb_triple_store
        elif choice == "2":
            server = triple_store
        elif choice == "3":
            print("Exiting...")
            break
        else:
            print("Invalid choice.")
            continue

        print("Choose an action:")
        print("1. Query")
        print("2. Update")
        print("3. Merge")
        print("4. Exit")

        action = input("Enter your choice: ")

        if action == "1":
            subject = input("Enter subject: ")
            predicate = input("Enter predicate: ")
            results = server.query(subject, predicate)
            print("Query Results:", results)
        elif action == "2":
            subject = input("Enter subject: ")
            predicate = input("Enter predicate: ")
            obj = input("Enter object: ")
            server.update(subject, predicate, obj)
            print("Update Successful")
        elif action == "3":
            server_id = input("Enter server ID: ")
            if server_id==server.server_id:
                print("Invalid can't merge the server with itself")
                continue
            elif (server_id!="mysql" and server_id!="mongo"):
                print("not a valid server")
                continue
            else:
                if isinstance(server, MySQLTripleStore):
                    server.merge(mongodb_triple_store)
                    mongodb_triple_store.merge(server)
                else:
                    server.merge(triple_store)
                    triple_store.merge(server)

            print("Merge Successful")
        elif action == "4":
            print("Exiting...")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
