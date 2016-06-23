import time
import dbUtil_networkLoading
import sys
import trafficAssignmentModel as TAM
import csv #Library for writing to CSV file, can be removed if this is not used


user = input('User: ')
password = input('Password: ')

user = "'" + user + "'"
password = "'" + password + "'"

[cur, conn] = dbUtil_networkLoading.dbConnect("'mode'", user, password)
#time_periods = dbUtil.getTimePeriods(cur)
process_start_time = time.time()
beta_values = dbUtil_networkLoading.getBetaValues(cur)
writer = csv.writer(open("file_name.csv", 'w'))
for beta in beta_values:
    print('Calculating route shares for beta = ', beta[0])
    routes = dbUtil_networkLoading.getVmRoutes(cur)
    shares = TAM.createRouteShares(routes, beta[0])
    for share in shares:
        writer.writerow(share)

    dbUtil_networkLoading.storeRouteShares(cur, conn, shares) #Make sure it is correct DB

process_elapsed_time = time.time() - process_start_time
print('')
print('Process done in: ', process_elapsed_time, 's')
