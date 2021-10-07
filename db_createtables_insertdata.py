import csv
from mysql.connector import cursor
from DbConnector import DbConnector
from tabulate import tabulate
import os
import pandas as pd


# get relevant users with activity data from textfile and return list with string user IDs
def relevant_users():
    with open('labeled_ids.txt') as file:
            lables = file.readlines()
            lables = [line.rstrip() for line in lables]
    return lables

# NOT USED ANYMORE
# creates dictioary with relevant Data to insert into the Activity table
def read_labels():
    labels = relevant_users()
    insert_dic = {}
    for (root,dirs,files) in os.walk('Data'):
        user = str(root[5:8])
        if user in labels:
            #print('User: ' + user)
            if files[0] == 'labels.txt':
                              
                with open(str(root)+'\labels.txt') as file:
                    activities = []
                    for line in csv.reader(file, delimiter='\t'): #You can also use delimiter="\t" rather than giving a dialect.
                        line.insert(0,user)
                        activities.append(tuple(line))
                    #delete the column titles    
                    activities.pop(0)
                #put everything into a big dictionary with key= userID and value is a list of list with the activity data
                insert_dic[user] = activities
        else:
            pass
    return insert_dic


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                   id varchar(255) NOT NULL PRIMARY KEY,
                   has_labels tinyint(1);
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint(self):
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    activity_id INT,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id));
                """
        self.cursor.execute(query)
        self.db_connection.commit()    

    def create_Activity(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    user_id varchar(256),
                    transportation_mode TEXT,
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id));
                """
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_activity_and_trackpoints(self):
        users = []
        for user in range(182):
            users.append(f"{user:03d}")

        for (root,dirs,files) in os.walk('Data'):
            user = str(root[5:8])
            # testing with user 6
            if user == '006':#in users: #in users:
                if root[9:] == "Trajectory":
                    print('Current User: ' , user)
                    # Now we are in the right folder
                    for filename in files:
                        # read file and skip the 6 headerrows - very important
                        pltfile = pd.read_csv(str(root)+ '\\' + filename, header=None, skiprows=[0,1,2,3,4,5])
                        pltfile = pltfile.values.tolist()

                        if len(pltfile) > 2500:
                            #print("More than 2500, skip this activity")
                            continue
                        else:
                            # insert activity here
                            activity_insert = [user, str(pltfile[0][5]) +' ' + str(pltfile[0][6]), str(pltfile[-1][5]) + ' ' + str(pltfile[-1][6])]
                            query = "INSERT INTO Activity (user_id, start_date_time, end_date_time) VALUES (%s, %s, %s)"
                            #self.cursor.execute(query, activity_insert)

                            #get the activity ID from the last insert
                            activity_id = self.cursor.lastrowid
                            # insert Trackpoints with the same activity ID
                            trackpoint_insert_list = []
                            for i in range(len(pltfile)):
                                trackpoint_insert = (activity_id, pltfile[i][0], pltfile[i][1], pltfile[i][3], pltfile[i][4], str(pltfile[i][5]) + ' ' + str(pltfile[i][6]))
                                trackpoint_insert_list.append(trackpoint_insert)
                            
                            trackpoint_query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                            #self.cursor.executemany(trackpoint_query, trackpoint_insert_list)
                            self.db_connection.commit()

    def match_activity_labels(self):
        labels = relevant_users()
        for (root,dirs,files) in os.walk('Data'):
            user = str(root[5:8])
            if user in labels:
                if files[0] == 'labels.txt':
                    print('Current User: ' + user)            
                    with open(str(root)+'\labels.txt') as file:
                        activities = []
                        for line in csv.reader(file, delimiter='\t'): #You can also use delimiter="\t" rather than giving a dialect.
                            line.insert(0,user)
                            activities.append(tuple(line))
                        #delete the column titles    
                        activities.pop(0)
                    # check for each activity in the labels.txt file    
                    for check in activities:
                        select_query = "SELECT * FROM Activity WHERE user_id = \'%s\' AND start_date_time = \'%s\' AND end_date_time = \'%s\' ;" % (check[0], check[1], check[2])
                        self.cursor.execute(select_query)
                        rows = self.cursor.fetchall()
                        #  if there is a match update this Activity with the transportation mode
                        if rows:
                            update_query = "UPDATE Activity SET transportation_mode = \'%s\' WHERE id = \'%s\';" % (check[3], rows[0][0])
                            self.cursor.execute(update_query)
                            self.db_connection.commit()
                            #print(update_query)                  
            else:
                pass
        return 

    # inserts all users and marks relevant ones with labeled_ids with true
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

    # Example code
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



def main():
    program = None
    try:
        program = ExampleProgram()
        #program.insert_activity_and_trackpoints()
        #program.match_activity_labels()
        #program.insert_activity()
        #program.create_Activity()
        #program.create_trackpoint()
        # program.insert_user(table_name="User")
        # program.insert_data(table_name="Person")
        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="Person")
        # Check that the table is dropped
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    #pass
    main()
