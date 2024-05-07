from datetime import datetime

import mysql.connector
from server_interface import TripleStore

class MySQLTripleStore(TripleStore):
    def __init__(self, dbname, user, password, host, port):
        self.server_id="sql"
        self.conn = mysql.connector.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()

    def query(self, subject, predicate):
        self.cur.execute("SELECT * FROM triples WHERE subject = %s and predicate = %s", (subject, predicate,))
        return self.cur.fetchall()
    def update_merge_log(self,server_id):
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("SELECT timestamp FROM mergelog WHERE server_id = %s ORDER BY timestamp DESC LIMIT 1", (server_id,))
        last_merge_timestamp = self.cur.fetchone()
        if last_merge_timestamp:
            h=self.cur.execute("UPDATE mergelog SET TIMESTAMP = %s where server_id=%s",(current_timestamp,server_id,))
            # print("h : ", h)
        else:
            self.cur.execute("INSERT INTO mergelog (server_id,timestamp) values (%s ,%s)",(server_id,current_timestamp,))
            self.cur.execute("select count(*) from mergelog;")
            h = self.cur.fetchone()
            print("h ", h)
        self.conn.commit()
    def fetch_logs(self,server_id):
        # server_id=server.server_type

        print("FETCHING THE LOGS FROM THE SQL SERVER")
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("SELECT timestamp FROM mergelog WHERE server_id = %s ORDER BY timestamp DESC LIMIT 1", (server_id,))
        last_merge_timestamp = self.cur.fetchone()
        triplets = []
        if last_merge_timestamp:
            last_merge_timestamp = last_merge_timestamp[0]

        # Fetch logs from the database
            self.cur.execute("SELECT subject,predicate,object,timestamp FROM log WHERE timestamp > %s", (last_merge_timestamp,))
            logs = self.cur.fetchall()
            # Create a list to store triplets
            # Iterate over each log entry and create triplets
            for log in logs:
                triplet = (log[0], log[1], log[2],logs[3])  # Assuming log is a tuple (subject, predicate, object)
                triplets.append(triplet)
        else:
            self.cur.execute("SELECT subject,predicate,object,timestamp FROM log")
            logs = self.cur.fetchall()
            # print("sql logs :", logs)
            # print("server_id :", server_id)
            # print("timestamp :", current_timestamp)
            # Create a list to store triplets

            # Iterate over each log entry and create triplets
            for log in logs:
                triplet = (log[0], log[1], log[2],log[3])  # Assuming log is a tuple (subject, predicate, object)
                triplets.append(triplet)
        self.update_merge_log(server_id)
        # print("triplets ", triplets)
        return triplets
    def update(self, subject, predicate, obj):
    # Get the current timestamp
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Check if the subject-predicate pair already exists in the triples table
        self.cur.execute("SELECT COUNT(*) FROM log WHERE subject = %s AND predicate = %s", (subject, predicate))
        result1 = self.cur.fetchone()[0]  # Fetch the count result

        self.cur.execute("SELECT COUNT(*) FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
        result = self.cur.fetchone()[0]  # Fetch the count result
        if (result>0):
            self.cur.execute("UPDATE triples SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s", (obj, current_timestamp, subject, predicate))
        else:
            self.cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
                             (subject, predicate, obj, current_timestamp))
            # self.cur.execute("INSERT INTO log (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
            #(subject, predicate, obj, current_timestamp))
            # Log the update in the update log tab
        if (result1>0):
            self.cur.execute("UPDATE log SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s", (obj, current_timestamp, subject, predicate))
        else:
            self.cur.execute("INSERT INTO log (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",(subject, predicate, obj, current_timestamp))
            # self.cur.execute("INSERT INTO log (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
            #(subject, predicate, obj, current_timestamp))
        # Log the update in the update log tab
        self.conn.commit()

    def merge(self, server_object):
        log_entries=server_object.fetch_logs(self.server_id)
        for log_entry in log_entries:
            subject, predicate, obj, timestamp = log_entry
            self.cur.execute("SELECT COUNT(*) FROM log WHERE subject = %s AND predicate = %s", (subject, predicate))
            result1 = self.cur.fetchone()[0]
            # Check if the entry already exists in the main table based on subject and predicate
            self.cur.execute("SELECT * FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
            existing_entry = self.cur.fetchone()

            if existing_entry:
                # Check if the existing entry's timestamp is older than the new log entry's timestamp
                print(existing_entry[3])
                if existing_entry[3] < timestamp:
                    # Entry exists and its timestamp is older, update it with the new object and timestamp
                    self.cur.execute("UPDATE triples SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s",
                                     (obj, timestamp, subject, predicate))
                    if (result1>0):
                        self.cur.execute("UPDATE log SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s", (obj, timestamp, subject, predicate))
                    else:
                        self.cur.execute("INSERT INTO log (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",(subject, predicate, obj, timestamp))
            else:
                # Entry does not exist, insert a new row into the main table
                self.cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
                                 (subject, predicate, obj, timestamp))
                self.cur.execute("INSERT INTO log (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",(subject, predicate, obj, timestamp))

        self.conn.commit()

    def load_tsv_file(self, file_path):
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("DELETE FROM mergelog")

        # Delete all entries from the log table
        self.cur.execute("DELETE FROM log")

        # Delete all entries from the triples table
        self.cur.execute("DELETE FROM triples")
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line in lines:
                data = line.strip().split(' ')
                if len(data) == 3:
                    subject, predicate, obj = data
                    self.cur.execute("SELECT COUNT(*) FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
                         # Subject-predicate pair already exists, update the object in the triples table
                    result = self.cur.fetchone()[0]  # Fetch the count result
                    if (result>0):
                        self.cur.execute("UPDATE triples SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s", (obj, current_timestamp, subject, predicate))
                    else:
                        self.cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
                                         (subject, predicate, obj, current_timestamp))
                else:
                    print(f"Ignore line: {line.strip()}. Not in the format 'subject predicate object'.")

        self.conn.commit()