__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'QL_ALBK'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

startDate = datetime(2014, 12, 1).date()         # This is the start of training period
endDate = datetime(2015, 4, 10).date()         # This is the end of training period
rankingDays = 15                                # This is the number of days for which ranking is done
initializationDays = 15                         # This is the number of days for which q_matrix is initilaized
liveDays = 30                                   # This is the number of days for which live trading is done

logFileName = "QLearningALBK"

maxProcesses = 3                                # This is the number of maximum processes

#-----------------------------------------------------------------------------------------------------------------------------------------
# These variables need to contain list values
alpha = [0.5, 0.4, 0.6, 0.2, 0.8]                         # This defines the weightage to long trades as compared to short trades while constructing reward matrix
gamma = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]                         # This defines the weightage of old data as compared to latest observations of reward matrix
maxGreedyLevel = [5]                 # This defines the number of future states for which reward is to be maximized in construction of q matrix
individualFactor = [10]               # This defines the factor of total asset which is to be allocated to each strategy
zeroRange = [0.002, 0.006, 0.01]                   # This determines the spread between states 0, 1, 2

#------------------------------------------------------------------------------------------------------------------------------------------

dummyIndividualId = -1               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 250000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 10000000            # This is the total asset deployed
trainingFactor = 2
trainingMaxTotalAsset = maxTotalAsset*trainingFactor        # This is the total asset deployed while training

dummyPerformance = -50000

performanceMonthlyOutfileNameBase = 'albk mtm monthly 15 15 30'
performanceOutfileNameBase = 'albk mtm 15 15 30'

latestIndividualTableBase = "latest_individual_table"
trainingTradesheetTableBase = "training_tradesheet_data_table"
trainingAssetTableBase = "training_asset_allocation_table"
rankingTableBase = "ranking_table"
dailyAssetTableBase = "asset_daily_allocation_table"
newTradesheetTableBase = "tradesheet_data_table"
assetTableBase = "asset_allocation_table"
qMatrixTableBase = "q_matrix_table"
reallocationTableBase = "reallocation_table"
performanceTableBase = "performance_table"
rankingWalkforwardTableBase = "ranking_walkforward_table"
dailyMtmTableBase = "daily_mtm_table"
