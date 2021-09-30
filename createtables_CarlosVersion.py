from time import strptime

import numpy as np
from datetime import datetime

from DbConnector import DbConnector
from tabulate import tabulate
import os






class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT NOT NULL PRIMARY KEY,
                   activity_id INT,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id) REFERENCES Activity(id));
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT NOT NULL PRIMARY KEY,
                   activity_id INT,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id) REFERENCES Activity(id));
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def insert_user(self, table_name):
        with open('labeled_ids.txt') as file:
            lables = file.readlines()
            lables = [line.rstrip() for line in lables]

        for user in range(182):
            user = f"{user:03d}"

            if user in lables:
                query = "INSERT INTO %s (id , has_labels) VALUES ('%s', 1)"
                self.cursor.execute(query % (table_name, user))
            else:
                query = "INSERT INTO %s (id, has_labels) VALUES ('%s', 0)"
                self.cursor.execute(query % (table_name, user))
        self.db_connection.commit()

        # names = ['Bobby', 'Mc', 'McSmack', 'Board']

        # for name in names:
        #     # Take note that the name is wrapped in '' --> '%s' because it is a string,
        #     # while an int would be %s etc
        #     query = "INSERT INTO %s (name) VALUES ('%s')"
        #     self.cursor.execute(query % (table_name, name))
        # self.db_connection.commit()

    def insert_activity(self, table_name):
        id_activity = 0
        for (root, dirs, files) in os.walk('Data'):
            # print(root)
            # print(dirs)
            # print(files)
            # if dirs is []:
            for file in files:
                if file == "labels.txt":
                    user = str(root[5:8])
                    print('--------------------------------')
                    path = root + "\\" + file
                    f = open(path)
                    next(f)
                    for i in f:
                        query = "INSERT INTO %s (id, user_id, transportation_mode, start_date_time, end_date_time) " \
                                "VALUES ('%s', '%s', '%s', '%s', '%s')"
                        self.cursor.execute(query % (table_name, id_activity, user, str(i[40:50]),
                                                     datetime.strptime(i[0:19], '%Y/%m/%d %H:%M:%S')
                                                    , datetime.strptime(i[20:39], '%Y/%m/%d %H:%M:%S')))
                        id_activity = id_activity+1
        self.db_connection.commit()
def main():
    program = None
    try:
        program = ExampleProgram()
        # program.create_table(table_name="TrackPoint")
        # program.insert_user(table_name="User")
        # program.insert_activity(table_name="Activity")
        # program.insert_data(table_name="Person")
        # program.fetch_data(table_name="Activity")
        # program.drop_table(table_name="Activity")

        # Check that the table is dropped
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()