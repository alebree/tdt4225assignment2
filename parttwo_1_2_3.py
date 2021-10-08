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
    queryMax = "SELECT a.user_id, COUNT(t.activity_id) as numberOfActivities FROM TrackPoint t INNER JOIN Activity a ON t.activity_id=a.id GROUP BY a.user_id ORDER BY COUNT(t.activity_id) DESC LIMIT 1;" 
    self.cursor.execute(queryMax)
    records = self.cursor.fetchall()
    print(records)
    queryMin = "SELECT a.user_id, COUNT(t.activity_id) as numberOfActivities FROM TrackPoint t INNER JOIN Activity a ON t.activity_id=a.id GROUP BY a.user_id ORDER BY COUNT(t.activity_id) ASC LIMIT 1;" 
    self.cursor.execute(queryMin)
    records = self.cursor.fetchall()
    print(records)
    queryAvg = "SELECT AVG(tr.count) FROM (select COUNT(*) as count FROM TrackPoint t INNER JOIN Activity a ON t.activity_id=a.id GROUP BY t.activity_id) tr;" 
    self.cursor.execute(queryAvg)
    records = self.cursor.fetchall()
    print(records)

#3. Find the top 10 users with the highest number of activities.
def highestNumberOfActivities(self):
    print("3. Find the top 10 users with the highest number of activities.")
    queryTop10 = "SELECT a.user_id, COUNT(t.activity_id) FROM TrackPoint t INNER JOIN Activity a ON t.activity_id=a.id GROUP BY a.user_id ORDER BY COUNT(t.activity_id) DESC LIMIT 10;" 
    self.cursor.execute(queryTop10)
    records = self.cursor.fetchall()
    print(records)
