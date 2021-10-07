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
    
    def task9b_user62(self):
        #get activities from user 62
        select_activities_62 = "Select id FROM (Select id, user_id, yearmonth FROM (Select id, user_id, EXTRACT(YEAR_MONTH FROM start_date_time) as yearmonth FROM Activity) as A) as B where (yearmonth = '200811' AND user_id = '062');"
        self.cursor.execute(select_activities_62)
        rows = self.cursor.fetchall()
        activity_ids = []
        for i in rows:
            activity_ids.append(i[0])
        #print(activity_ids)
        import datetime
        totaltime = datetime.timedelta(0,0,0)
        for id in activity_ids:
            select_time_62 = "Select * FROM Activity Where id = %s;" %(id)
            self.cursor.execute(select_time_62)
            rows = self.cursor.fetchall()
            timedelta = rows[0][4]-rows[0][3]
            totaltime += timedelta
        return print('Total hours recorded user 62: ', totaltime)

    def task9b_user128(self):
        #get activities from user 128
        select_activities_62 = "Select id FROM (Select id, user_id, yearmonth FROM (Select id, user_id, EXTRACT(YEAR_MONTH FROM start_date_time) as yearmonth FROM Activity) as A) as B where (yearmonth = '200811' AND user_id = '128');"
        self.cursor.execute(select_activities_62)
        rows = self.cursor.fetchall()
        activity_ids = []
        for i in rows:
            activity_ids.append(i[0])
        #print(activity_ids)
        import datetime
        totaltime = datetime.timedelta(0,0,0)
        for id in activity_ids:
            select_time_128 = "Select * FROM Activity Where id = %s;" %(id)
            self.cursor.execute(select_time_128)
            rows = self.cursor.fetchall()
            timedelta = rows[0][4]-rows[0][3]
            totaltime += timedelta
        return print('Total hours recorded user 128: ', totaltime)        

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
 
    # only sum up altitude when next TrackPoint is higher than the one before
    def task11(self):
        user_altitude_dic = {}
        # First get all activites from one user
        for user in range(182):
            user = f"{user:03d}"
            activity_query = "Select id from Activity where user_id = '%s'" % (user)
            self.cursor.execute(activity_query)
            activity_ids = self.cursor.fetchall()

            total_altitude_user = 0
            # Get the altitudes for every activity of the User
            for activity in activity_ids:
                select_query = "Select altitude FROM TrackPoint where activity_id = %s;" % (activity[0])
                self.cursor.execute(select_query)
                rows = self.cursor.fetchall()
        
                total_altitude_activity = 0
                
                for i in range(0, len(rows)):
                    if i == 0:
                        continue
                    #print(rows[i][0])
                    if rows[i][0] == -777:
                        continue
                    if rows[i][0] > rows[i-1][0]:
                        #print(rows[i][0])
                        total_altitude_activity += rows[i][0] - rows[i-1][0]
                    
                # add the gained altitude of every activity of the user to the total user gained altitude
                total_altitude_user += total_altitude_activity
                #convert feet to meters for the solution
                total_altitude_user = total_altitude_user/3.2808
            # add all user to a dictionary 
            user_altitude_dic[user] = total_altitude_user
        
        sorted_dic = dict(sorted(user_altitude_dic.items(), key=lambda item: item[1], reverse=True))
        return print('Solution in decending order: ', sorted_dic)


    # 12. Find all users who have invalid activities, and the number of invalid activities per user
    def task12(self):
        print("12. Find all users who have invalid activities, and the number of invalid activities per user")
        query1 = """select activity_id from TrackPoint"""
        self.cursor.execute(query1)
        query1_results = self.cursor.fetchall()
        list_of_activities = []
        list_of_users = []
        for i in query1_results:
            query2 = """select date_days from TrackPoint where activity_id=\'%s\'""" % (i[0])
            self.cursor.execute(query2)
            query2_results = self.cursor.fetchall()
            for j in range(0, len(query2_results)-1):
                k = j + 1

                if query2_results[j][0] > query2_results[k][0]+0.003472224 \
                        or query2_results[j][0] < query2_results[k][0]-0.003472224:
                        list_of_activities.append(i[0])

        for l in list_of_activities:
            query3 = """select id_user from Activity where id = \'%s\'""" % (l[0])
            self.cursor.execute(query3)
            query3_results = self.cursor.fetchall()
            list_of_users.append(query3_results[0])

def main():
    program = None
    try:
        program = parttwo()
        #program.task9b_user62()
        #program.task9b_user128()
        program.task12()
        #program.match_activity_labels()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()            