from datetime import datetime
from geopy.distance import geodesic
# 6. Find the number of users which have been close to each other in time andspace (Covid-19 tracking).
# Close is defined as the same minute (60 seconds)and space (100 meters).


def show_covid_users(self):
        print("Find the number of users which have been close to each other in time andspace (Covid-19 tracking). "
              "Close is defined as the same minute (60 seconds)and space (100 meters).")
        lat_query = """select lat from TrackPoint"""
        self.cursor.execute(lat_query)
        lat = self.cursor.fetchall()
        # We take the lat value of the TrackPoint
        lon_query = """select lon from TrackPoint"""
        self.cursor.execute(lon_query)
        lon = self.cursor.fetchall()
        # We take the lon value of the TrackPoint
        trackpointsclose=[]
        for i in list(lat):
            for j in list(lon):
                dot1 = (i[0], j[0])
                # We make a dot with the latitude and longitude values
                for l in list(lat):
                    for k in list(lon):
                        dot2 = (l[0], k[0])
                        # We make a second dot
                        if geodesic(dot1, dot2).m < 100 and dot1 != dot2:
                            # This function returns the distance between two dots, if the distance is less than 100,
                            # then append the dot into a list of closed dots
                            trackpointsclose.append(dot1)

        for i in trackpointsclose:
            closepoints = """select activity_id from TrackPoint where lat=\'%f\' and lon=\'%f\' group by activity_id""",\
                        i[0], i[1]
            self.cursor.execute(closepoints)
            activity_id = self.cursor.fetchall()
            # We get the activities that are close to another, so we can get the users later
        self.take_dates_withinhour()
        # When we have the list of activities that are close in space and list of activities that are close in time,
        # we just find the common results of both and then find the users corresponding with the activities.
        # Due to an excessive long time of running program, we have not been able to check the previous parts of the query


def take_dates_withinhour(self):
        start_query = """select start_date_time, end_date_time from Activity"""
        self.cursor.execute(start_query)
        start = self.cursor.fetchall()
        activities_close = []
        for i in start:
            for j in start:
                s1 = str(i[0])
                s2 = str(j[0])
                s3 = str(i[1])
                s4 = str(j[1])
                FMT = '%Y-%m-%d %H:%M:%S'
                s1date = datetime.strptime(s1, FMT)
                s2date = datetime.strptime(s2, FMT)
                s3date = datetime.strptime(s3, FMT)
                s4date = datetime.strptime(s4, FMT)
                if s1 != s2:
                    result = s2date - s1date
                    if result.days == 0 and ((s1date + timedelta(hours=1)) > s2date or
                                             (s1date + timedelta(hours=-1)) < s2date):
                        activities_close.append(s1date)
        for l in activities_close:
            query3 = """select id_user from Activity where id = '%s\'""" % (activities_close[l])
            self.cursor.execute(query3)
            query3_results = self.cursor.fetchall()
        return query3_results
