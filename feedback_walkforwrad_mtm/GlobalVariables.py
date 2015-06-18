__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'QL_AXIS'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

startDate = datetime(2012, 1, 2).date()         # This is the start of training period
endDate = datetime(2012, 12, 31).date()           # This is the end of training period

alpha = 0.5                         # This defines the weightage to long trades as compared to short trades while constructing reward matrix
gamma = 0.75                         # This defines the weightage of old data as compared to latest observations of reward matrix
maxGreedyLevel = 10
dummyIndividualId = -1               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 250000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 10000000            # This is the total asset deployed
trainingFactor = 2
trainingMaxTotalAsset = maxTotalAsset*trainingFactor        # This is the total asset deployed while training
factor = 10
maxAsset = maxTotalAsset/factor     # This is the maximum asset an individual can use
zeroRange = 0.005                   # This determines the spread between states 0, 1, 2

maxProcesses = 2                     # This is the maximum number of threads that can run concurrently
dummyPerformance = -50000

performanceMonthlyOutfileName = 'performance monthly.csv'
performanceOutfileName = 'performance.csv'