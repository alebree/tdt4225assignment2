# 12. Find all users who have invalid activities, and the number of invalid activities per user
    def invalid_activities(self):
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