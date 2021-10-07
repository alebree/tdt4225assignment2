from mysql.connector import cursor
from DbConnector import DbConnector
import geopy.distance

class parttwo:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # Find the number of users that have started the activity i n one day and ended the activity the next day.
    # QUERY WAS DONE SOLEY IN SQL, code is just the documentation
    def task4(self):
        # to get full list of Activity that start on one day and end on the next
        first_select_query = "SELECT * FROM (SELECT *, end-start as dif FROM (Select id, user_id, cast(start_date_time as date) as start, cast(end_date_time as date) as end FROM Activity) as A) as B WHERE dif = 1;"
        # count disctinct users
        select_query = "SELECT COUNT(DISTINCT user_id) as solution FROM (SELECT *, end-start as dif FROM (Select id, user_id, cast(start_date_time as date) as start, cast(end_date_time as date) as end FROM Activity) as A) as B WHERE dif = 1;"
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        print("Solution is ", rows[0][0])
        return
    
    def task10(self):
        select_query = "Select * FROM Activity WHERE (user_id = 112 AND transportation_mode = 'walk');" # All Activities in 2008
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        activity_ids = []
        for i in rows:
            activity_ids.append(i[0])
        print(activity_ids)
        
        total_distance = 0
        # interate over all relevant activities
        for i in activity_ids:
            gps_points = []
            print(i)

            select_query_trackPoint = "Select * FROM TrackPoint WHERE activity_id = %s;" % (i)
            self.cursor.execute(select_query_trackPoint)
            tpoints = self.cursor.fetchall()
            for j in tpoints:
                gps_points.append((j[2],j[3]))
            gps_points = tuple(gps_points)

            distance_activity = 0
            for count in range(len(gps_points)-1):
                distance_activity += geopy.distance.distance(gps_points[count], gps_points[count+1]).km
            total_distance += distance_activity
        return print('Total distance walked: ', total_distance)

def main():
    program = None
    try:
        program = parttwo()
        program.task10()
        #program.match_activity_labels()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()            