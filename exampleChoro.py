from datetime import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine
class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        
    
    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id int NOT NULL PRIMARY KEY,
                   activity_id int,
                   foreign key (activity_id) references Activity(id),
                   date_time datetime,
                   lat double,
                   lon double,
                   altitude int,
                   date_days double)
                """

                
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_data(self, table_name):
        #onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        
        print(os.path.abspath('../../dataset/dataset/Data'))

        # for name in onlyfiles:
        #     # Take note that the name is wrapped in '' --> '%s' because it is a string,
        #     # while an int would be %s etc
        #     query = "INSERT INTO %s (name) VALUES ('%s')"
        #     self.cursor.execute(query % (table_name, name))
        # self.db_connection.commit()

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
        with open('../../dataset/dataset/labeled_ids.txt') as file:
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

    def insert_activity(self):
        with open('../../dataset/dataset/labeled_ids.txt') as file:
            lables = file.readlines()
            lables = [line.rstrip() for line in lables]
        for (root, dirs, files) in os.walk('../../dataset/dataset/Data'):
             break
        id_activity = 0
        for dir in dirs:
            for label in lables:
                if (dir == label):
                    with open(os.path.join("../../dataset/dataset/Data", dir, "labels.txt")) as file:
                        df = pd.read_csv(file, sep="\t",  parse_dates=['Start Time', 'End Time'], 
    date_parser=lambda x: pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S'))
                        df.insert(0, 'id', range(id_activity, id_activity + len(df)))
                        df.insert(1, "user_id", dir, allow_duplicates=True)
                        df = df.rename(columns={'Start Time': 'start_date_time', 'End Time': 'end_date_time', 'Transportation Mode':'transportation_mode'})

                        engine = create_engine("mysql+pymysql://" + os.environ.get('DB_USER') + ":" + os.environ.get('DB_PASS') + "@" + "tdt4225-27.idi.ntnu.no" + "/" + "dbForTest")
                        df.to_sql(name="Activity", con=engine, if_exists="append", index = False)
                        self.db_connection.commit()
                        id_activity = id_activity + len(df)+1

    def insert_trackPoint(self):
            
        activities = self.getActivityTable()
        # for row in activities:
        #     print(row[0])

        for (root, dirs, files) in os.walk('../../dataset/dataset/Data'):
             break
        id_trackPoint = 0
        
        for dir in dirs:
            for label in activities:
                
                if (dir == label[2]):
                    for (root, dirs, files) in os.walk(os.path.join('../../dataset/dataset/Data', dir, "Trajectory")):
                        
                        
                        for i in  files:
                            filepath = os.path.join('../../dataset/dataset/Data', dir, "Trajectory", i)
                            df = pd.read_csv(filepath, sep=",", skiprows=6, header=None,
                         parse_dates=[[5, 6]], infer_datetime_format=True)
                            
                            if (df.size< 2500):
                                
                                #problems: add column names, 
                                print(label[2])
                                if(df['5_6'].iloc[0] == label[3] and df['5_6'].iloc[-1] == label[4]):
                                    df.drop(['2'])
                                    df = df.rename(columns={'5_6': 'date_time', '0': 'lat', '1':'lon', '3':'altitude', '4':'date_days'})
                                    
                                    
                                    df.insert(0, 'id', range(id_trackPoint, id_trackPoint + len(df)))
                                    df.insert(1, "activity_id", dir, allow_duplicates=True)
                                    print(df.head())
                                    id_trackPoint = id_trackPoint + len(df)+1
                                    
                                if(df['5_6'].iloc[0] != label[3] and df['5_6'].iloc[-1] != label[4]):
                                    # df.drop(['2'])
                                    # df = df.rename(columns={'5_6': 'date_time', '0': 'lat', '1':'lon', '3':'altitude', '4':'date_days'})
                                    # print(df.head())
                                    # df.insert(0, 'id', range(id_trackPoint, id_trackPoint + len(df)))
                                    
                                    id_trackPoint = id_trackPoint + len(df)+1
                            
                            #rest see insert_activity
                  
                


    def getActivityTable(self):
        query = "Select * from Activity"
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        
        self.db_connection.commit()
        return records

    #1. How many users, activities and trackpoints are there in the dataset (after it is
    #inserted into the database).
    def getCounts(self):
        print("1. How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).")
        queryCountUser = "Select count(*) from User"
        self.cursor.execute(queryCountUser)
        records = self.cursor.fetchall()
        print(records)
        queryCountActivity = "Select count(*) from Activity"
        self.cursor.execute(queryCountActivity)
        records = self.cursor.fetchall()
        print(records)
        queryCountTrackPoint = "Select count(*) from TrackPoint"
        self.cursor.execute(queryCountTrackPoint)
        records = self.cursor.fetchall()
        print(records)

    #2. Find the average, minimum and maximum number of activities per user.
    def getAverageMinMax(self):
        print("2. Find the average, minimum and maximum number of activities per user.")
        #add a join to table Activity to get user_id and select only activities that have a user_id?
        queryMax = "select activity_id, count(activity_id) from TrackPoint group by activity_id order by count(activity_id) desc limit 1;" 
        self.cursor.execute(queryMax)
        records = self.cursor.fetchall()
        print(records)
        #add a join to table Activity to get user_id and select only activities that have a user_id?
        queryMin = "select activity_id, count(activity_id) from TrackPoint group by activity_id order by count(activity_id) asc limit 1;" 
        self.cursor.execute(queryMin)
        records = self.cursor.fetchall()
        print(records)
        queryAvg = "select avg(a.count) from (select count(*) as count from TrackPoint ac group by ac.activity_id) a" 
        self.cursor.execute(queryAvg)
        records = self.cursor.fetchall()
        print(records)
    
    #3. Find the top 10 users with the highest number of activities.
    def highestNumberOfActivities(self):
        print("3. Find the top 10 users with the highest number of activities.")
        queryTop10 = "select activity_id, count(activity_id) from TrackPoint group by activity_id order by count(activity_id) desc limit 10;" 
        self.cursor.execute(queryTop10)
        records = self.cursor.fetchall()
        print(records)
    
    # 4. Find the number of users that have started the activity in one day and ended
    # the activity the next day.
    def numberOfUserStartEnd():
        print("4. Find the number of users that have started the activity in one day and ended the activity the next day.")


def main():
    program = None
    try:
        program = ExampleProgram()
        #program.insert_user(table_name="User")
        #program.create_table(table_name="TrackPoint")
        #program.insert_data(table_name="Activity")
        #_ = program.fetch_data(table_name="User")
        #program.drop_table(table_name="User")
        # Check that the table is dropped
        #program.insert_activity()
        #program.getActivityTable()
        program.insert_trackPoint()
        #program.show_tables()
        # program.getCounts()
        # program.getAverageMinMax()
        # program.highestNumberOfActivities()
        #program.numberOfUserStartEnd()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
