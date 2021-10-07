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