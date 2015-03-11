__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'MKT'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
startDate = datetime(2012, 1, 2).date()         # This is the start of trading period
endDate = datetime(2012, 12, 30).date()           # This is the end of trading period
alpha = 0.8                         # This defines the weightage to long trades as compared to short trades while constructing reward matrix
gamma = 0.9                         # This defines the weightage of old data as compared to latest observations of reward matrix
maxGreedyLevel = 3
dummyIndividualId = -1               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 250000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 10000000            # This is the total asset deployed
trainingFactor = 2
trainingMaxTotalAsset = maxTotalAsset*trainingFactor        # This is the total asset deployed while training
factor = 2
maxAsset = maxTotalAsset/factor     # This is the maximum asset an individual can use
zeroRange = 0.001                   # This determines the spread between states 0, 1, 2
aggregationUnit = 1
maxThreads = 8                     # This is the4 maximum number of threads that can run concurrently