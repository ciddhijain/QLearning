__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from decimal import Decimal

class DBUtils:

    databaseObject = None
    alpha = None
    gamma = None
    individualFactor = None
    zeroRange = None
    greedyLevel = None
    individualMaxAsset = None
    latestIndividualTable = None
    trainingTradesheetTable = None
    trainingAssetTable = None
    rankingTable = None
    dailyAssetTable = None
    newTradesheetTable = None
    assetTable = None
    qMatrixTable = None
    reallocationTable = None
    performanceTable = None

    def __init__(self, alpha_local=0, gamma_local=0, individualFactor_local=1, zeroRange_local=0, greedyLevel_local=0,
                 latestIndividualTable_local="", trainingTradesheetTable_local="", trainingAssetTable_local="", qMatrixTable_local="",
                 reallocationTable_local="", assetTable_local="", dailyAssetTable_local="", newTradesheetTable_local=""):
        global alpha
        global gamma
        global individualFactor
        global zeroRange
        global greedyLevel
        global individualMaxAsset
        global latestIndividualTable
        global trainingAssetTable
        global trainingTradesheetTable
        global rankingTable
        global dailyAssetTable
        global newTradesheetTable
        global assetTable
        global qMatrixTable
        global reallocationTable
        global performanceTable

        alpha = alpha_local
        gamma = gamma_local
        individualFactor = individualFactor_local
        zeroRange = zeroRange_local
        greedyLevel = greedyLevel_local
        individualMaxAsset = gv.maxTotalAsset / individualFactor
        latestIndividualTable = latestIndividualTable_local
        trainingTradesheetTable = trainingTradesheetTable_local
        trainingAssetTable = trainingAssetTable_local
        rankingTable = gv.rankingTableBase
        dailyAssetTable = dailyAssetTable_local
        newTradesheetTable = newTradesheetTable_local
        assetTable = assetTable_local
        qMatrixTable = qMatrixTable_local
        reallocationTable = reallocationTable_local
        performanceTable = gv.performanceTableBase

    def dbConnect (self):
        db_username = gv.userName
        db_password = gv.password
        db_host = gv.dbHost
        db_name = gv.databaseName
        db_port = gv.dbPort
        db_connector = gv.dbConnector
        global databaseObject
        databaseObject = DatabaseManager(db_connector, db_username, db_password,db_host,db_port, db_name)
        databaseObject.Connect()

    def dbQuery (self, query):
        global databaseObject
        return databaseObject.Execute(query)

    def dbClose (self):
        global databaseObject
        databaseObject.Close()

    # Function to check if given day is a trading day
    def checkTradingDay(self, date):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "'), 1"
        return databaseObject.Execute(queryCheck)

    # Function to insert new trade in tradesheet
    def insertNewTrade(self, tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice):
        global databaseObject
        global newTradesheetTable
        queryInsertTrade = "INSERT INTO " + newTradesheetTable + \
                           " (trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time, exit_price)" \
                           " VALUES" \
                           " (" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + ", '" + str(entryDate) + "', '" + str(entryTime) +\
                           "', " + str(entryPrice) + ", " + str(entryQty) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ")"
        #print(queryInsertTrade)
        databaseObject.Execute(queryInsertTrade)

    # Function to insert new trade in training_tradesheet
    def insertTrainingNewTrade(self, tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice):
        global databaseObject
        global trainingTradesheetTable
        queryInsertTrade = "INSERT INTO " + trainingTradesheetTable + \
                           " (trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time, exit_price)" \
                           " VALUES" \
                           " (" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + ", '" + str(entryDate) + "', '" + str(entryTime) +\
                           "', " + str(entryPrice) + ", " + str(entryQty) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ")"
        #print(queryInsertTrade)
        databaseObject.Execute(queryInsertTrade)

    # Function to get individuals which have active trades in a given interval of time on a given day
    def getIndividuals (self, startDate, startTime, endDate, endTime):
        global databaseObject
        global newTradesheetTable
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM " + newTradesheetTable + " WHERE entry_time<'" + str(endTime) + \
                           "' AND exit_time>'" + str(startTime) + "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryIndividuals)

    # Function to get individuals which have active trades in a given interval of time on a given day during training
    def getTrainingIndividuals (self, startDate, startTime, endDate, endTime):
        global databaseObject
        global trainingTradesheetTable
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM " + trainingTradesheetTable + " WHERE entry_time<'" + str(endTime) + \
                           "' AND exit_time>'" + str(startTime) + "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryIndividuals)

    # Function to get individuals from original tradesheet in a given interval of dates
    def getRefIndividuals(self, startDate, endDate):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                           "' AND entry_date<='" + str(endDate) + "'"
        return databaseObject.Execute(queryIndividuals)

    # Function to get all individuals from original tradesheet
    def getAllIndividuals(self):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        return databaseObject.Execute(queryIndividuals)

    # Function to get trades that are active in a given time interval
    def getTrades (self, startDate, startTime, endDate, endTime):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time " \
                      "FROM " + newTradesheetTable + " WHERE entry_time<='" + str(endTime) + "' AND exit_time>='" + str(startTime) + \
                      "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get new trades from original tradesheet
    def getRefDayTrades (self, date):
        global databaseObject
        queryTrades = "SELECT * FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "'"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    # Function to add or update mtm for an individual in daily mtm table
    def insertOrUpdateMTM(self, individualId, date, mtm):
        global databaseObject
        queryCheck = "SELECT EXISTS ( SELECT 1 FROM " + gv.dailyMtmTableBase + " WHERE individual_id="+ str(individualId) + \
                     " AND mtm_date='" + str(date) + "' ), 1"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==1:
                query = "UPDATE " + gv.dailyMtmTableBase + " SET mtm=mtm+" + str(mtm) + " WHERE individual_id=" + str(individualId) + \
                        " AND date='" + str(date) + "'"
                databaseObject.Execute(query)
            else:
                query = "INSERT INTO " + gv.dailyMtmTableBase + \
                        " ( individual_id, mtm, mtm_date )" \
                        " VALUES" \
                        " ( " + str(individualId) + ", " + str(mtm) + ", '" + str(date) + "' )"
                databaseObject.Execute(query)
        return

    # Function to return mtm for a given date
    def getDailyMTM(self, individualId, date):
        global databaseObject
        query = "SELECT mtm, 1 FROM " + gv.dailyMtmTableBase + " WHERE mtm_date='" + str(date) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(query)

    # Function to get new trades from original tradesheet based on ranking
    def getRankedTradesOrdered (self, date, startTime, endTime, walkforward):
        global databaseObject
        global rankingTable
        queryTrades = "SELECT t.* FROM old_tradesheet_data_table AS t JOIN " + rankingTable + " as r ON t.individual_id=r.individual_id" \
                      " WHERE t.entry_date='" + str(date) + "' AND t.entry_time<'" + str(endTime) + "' AND t.entry_time>='" + str(startTime) + \
                      "' AND r.ranking_walkforward_id=" + str(walkforward) + " ORDER BY t.entry_time, r.ranking"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    # Function to get trades taken by an individual in an interval
    def getTradesIndividual(self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT * FROM " + newTradesheetTable + " WHERE entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + \
                      "' AND exit_time>='" + str(startTime) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades taken by an individual in an interval during training
    def getTrainingTradesIndividual(self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        global trainingTradesheetTable
        queryTrades = "SELECT * FROM " + trainingTradesheetTable + " WHERE entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + \
                      "' AND exit_time>='" + str(startTime) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval
    def getTradesExit(self, date, startTime, endTime):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM " + newTradesheetTable + " WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end
    def getTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM " + newTradesheetTable + " WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval during training
    def getTrainingTradesExit(self, date, startTime, endTime):
        global databaseObject
        global trainingTradesheetTable
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM " + trainingTradesheetTable + " WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end during training
    def getTrainingTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        global trainingTradesheetTable
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM " + trainingTradesheetTable + " WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get price series in a time range
    # Not being used currently
    def getPriceSeries (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryPriceSeries = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time>='" + str(startTime) + \
                           "' AND time<='" + str(endTime) + "'"
        return databaseObject.Execute(queryPriceSeries)

    # Function to get price from price series for a given date and time
    def getPrice(self, startDate, startTime):
        global databaseObject
        queryPrice = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time='" + str(startTime) + "'"
        return databaseObject.Execute(queryPrice)

    # Function to get Q Matrix of an individual
    def getQMatrix (self, individualId):
        global databaseObject
        global qMatrixTable
        queryQM = "SELECT row_num, column_num, q_value FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryQM)

    # Function to insert / update Q matrix of an individual
    def updateQMatrix(self, individualId, qm):
        global databaseObject
        global qMatrixTable
        queryCheck = "SELECT EXISTS (SELECT 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + "), 1"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==1:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryUpdate = "UPDATE " + qMatrixTable + " SET q_value=" + str(round(qm[i,j], 10)) + " WHERE individual_id=" + str(individualId) + \
                                      " AND row_num=" + str(i) + " AND column_num=" + str(j)
                        databaseObject.Execute(queryUpdate)
            else:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryInsert = "INSERT INTO " + qMatrixTable + \
                                     " (individual_id, row_num, column_num, q_value)" \
                                     " VALUES " \
                                     " (" + str(individualId) + ", " + str(i) + ", " + str(j) + ", " + str(round(qm[i,j], 10)) + ")"
                        databaseObject.Execute(queryInsert)

    # Function to insert individual entry in assetTable
    def addIndividualAsset (self, individualId, usedAsset):
        global databaseObject
        global assetTable
        global individualMaxAsset
        queryAddAsset = "INSERT INTO " + assetTable + \
                        "(individual_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(individualId) + ", " + str(round(individualMaxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((individualMaxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert individual entry in trainingAssetTable
    def addTrainingIndividualAsset (self, individualId, usedAsset):
        global databaseObject
        global trainingAssetTable
        global individualMaxAsset
        queryAddAsset = "INSERT INTO " + trainingAssetTable + \
                        "(individual_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(individualId) + ", " + str(round(individualMaxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((individualMaxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to check if an individual's entry exists in assetTable
    def checkIndividualAssetExists (self, individualId):
        global databaseObject
        global assetTable
        queryCheck = "SELECT EXISTS (SELECT 1 FROM " + assetTable + " WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in trainingAssetTable
    def checkTrainingIndividualAssetExists (self, individualId):
        global databaseObject
        global trainingAssetTable
        queryCheck = "SELECT EXISTS (SELECT 1 FROM " + trainingAssetTable + " WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to update individual's asset
    def updateIndividualAsset(self, individualId, toBeUsedAsset):
        global databaseObject
        global assetTable
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM " + assetTable + " WHERE individual_id=" + str(individualId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE " + assetTable + " SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE individual_id=" + str(individualId)
            return databaseObject.Execute(queryUpdate)

    # Function to update individual's asset during training
    def updateTrainingIndividualAsset(self, individualId, toBeUsedAsset):
        global databaseObject
        global trainingAssetTable
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM " + trainingAssetTable + " WHERE individual_id=" + str(individualId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE " + trainingAssetTable + " SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE individual_id=" + str(individualId)
            return databaseObject.Execute(queryUpdate)

    # Function to get the asset being used by an individual at a given time
    # Not used currently
    def getUsedAsset (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        global newTradesheetTable
        queryUsedAsset = "SELECT entry_qty*entry_price, 1 FROM " + newTradesheetTable + " WHERE individual_id=" + str(individualId) + \
                         " AND entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + "' AND exit_time>'" + str(endTime) + "'"
        return databaseObject.Execute(queryUsedAsset)

    # Function to add individual's entry in reallocation table
    def addNewState(self, individualId, date, time, state):
        global databaseObject
        global reallocationTable
        queryNewState = "INSERT INTO " + reallocationTable + \
                        " (individual_id, last_reallocation_date, last_reallocation_time, last_state)" \
                        " VALUES" \
                        " (" + str(individualId) + ", '" + str(date) + "', '" + str(time) + "', " + str(state) + ")"
        return databaseObject.Execute(queryNewState)

    # Function to get last state for an individual
    def getLastState (self, individualId):
        global databaseObject
        global reallocationTable
        queryLastState = "SELECT last_state, individual_id FROM " + reallocationTable + " WHERE individual_id=" + str(individualId) + \
                         " AND last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM " + reallocationTable + " WHERE " \
                         "individual_id=" + str(individualId) + ") AND last_reallocation_time=(SELECT MAX(last_reallocation_time) " \
                        "FROM " + reallocationTable + " WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                        "(SELECT MAX(last_reallocation_date) FROM " + reallocationTable + " WHERE individual_id=" + str(individualId) + "))"
        return databaseObject.Execute(queryLastState)

    # Function to get next state for an individual
    def getNextState (self, individualId, currentState):
        global databaseObject
        global qMatrixTable
        global zeroRange
        queryMaxQValue = "SELECT MAX(q_value), 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + " AND row_num=" + str(currentState)
        resultMaxQValue = databaseObject.Execute(queryMaxQValue)
        queryCurrentQValue = "SELECT q_value, 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + " AND row_num=" + str(currentState) + \
                             " AND column_num=1"
        resultCurrentQValue = databaseObject.Execute(queryCurrentQValue)
        for maxQValue, dummy1 in resultMaxQValue:
            for currentQValue, dummy2 in resultCurrentQValue:
                # Checking with help of percentage difference between the maximum and current Q value
                if currentQValue!=0:
                    diff = float(abs(maxQValue-currentQValue)/currentQValue*100)
                    if diff>zeroRange:
                        queryNextState = "SELECT column_num, 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT MAX(q_value) FROM " + qMatrixTable + " WHERE individual_id=" + \
                                         str(individualId) + " AND row_num=" + str(currentState) + ")"
                        return databaseObject.Execute(queryNextState)
                    else:
                        queryNextState = "SELECT column_num, 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT q_value FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + \
                                         " AND row_num=" + str(currentState) + " AND column_num=1)"
                        return databaseObject.Execute(queryNextState)
                else:
                    queryNextState = "SELECT column_num, 1 FROM " + qMatrixTable + " WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                     str(currentState) + " AND column_num=1"
                    return databaseObject.Execute(queryNextState)

    # Function to reduce free asset for an individual
    def reduceFreeAsset(self, individualId, unitQty):
        global databaseObject
        global assetTable
        resultCurrentFreeAsset = databaseObject.Execute("SELECT free_asset, total_asset FROM " + assetTable +
                                                        " WHERE individual_id="+str(individualId))
        for freeAsset, totalAsset in resultCurrentFreeAsset:
            if (float(freeAsset)>=unitQty):
                newFreeAsset = float(freeAsset) - unitQty
                newTotalAsset = float(totalAsset) - unitQty
                queryUpdate = "UPDATE " + assetTable + " SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = float(totalAsset - freeAsset)
                queryUpdate = "UPDATE " + assetTable + " SET free_asset=0, total_asset=" + str(round(newTotalAsset,4)) + \
                              " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)

    # Function to increase free asset for an individual
    def increaseFreeAsset(self, individualId, unitQty):
        global databaseObject
        global assetTable
        global individualMaxAsset
        resultCurrentTotalAsset = databaseObject.Execute("SELECT total_asset, free_asset FROM " + assetTable +
                                                        " WHERE individual_id=" + str(individualId))
        for totalAsset, freeAsset in resultCurrentTotalAsset:
            newTotalAsset = float(totalAsset) + unitQty
            newFreeAsset = float(freeAsset) + unitQty
            if newTotalAsset<=individualMaxAsset:
                queryUpdate = "UPDATE " + assetTable + " SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                #print(queryUpdate)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = individualMaxAsset
                newFreeAsset = float(freeAsset) + individualMaxAsset - float(totalAsset)
                queryUpdate = "UPDATE " + assetTable + " SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                #print(queryUpdate)
                return databaseObject.Execute(queryUpdate)

    # Function to get current free asset for an individual
    def getFreeAsset(self, individualId):
        global databaseObject
        global assetTable
        queryCheck = "SELECT free_asset, 1 FROM " + assetTable + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)

    # Function to get current free asset for an individual during training
    def getTrainingFreeAsset(self, individualId):
        global databaseObject
        global trainingAssetTable
        queryCheck = "SELECT free_asset, 1 FROM " + trainingAssetTable + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)

    # Function to reset assetTable at the beginning
    def resetAssetAllocation(self, date, time):
        global databaseObject
        global trainingAssetTable
        global dailyAssetTable
        global assetTable
        databaseObject.Execute("INSERT INTO " + assetTable +
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.maxTotalAsset,4)) + ", 0, " + str(round(gv.maxTotalAsset,4)) + ")")
        databaseObject.Execute("INSERT INTO " + dailyAssetTable +
                               " (date, time, total_asset)"
                               " VALUES"
                               " ('" + str(date) + "', '" + str(time) + "', " + str(round(gv.maxTotalAsset, 4)) + ")")
        databaseObject.Execute("INSERT INTO " + trainingAssetTable +
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to insert free asset at day end into dailyAssetTable
    def insertDailyAsset(self, date, time):
        global databaseObject
        global dailyAssetTable
        global assetTable
        resultAsset = databaseObject.Execute("SELECT free_asset, 1 from " + assetTable + " where individual_id=" + str(gv.dummyIndividualId))
        for totalAsset, dummy in resultAsset:
            databaseObject.Execute("INSERT INTO " + dailyAssetTable +
                                   " (date, time, total_asset)"
                                   " VALUES"
                                   " ('" + str(date) + "', '" + str(time) + "', " + str(totalAsset) + ")")

    # Function to return Net Profit-Loss of Long trades within an interval
    def getLongNetPL(self, startDate, endDate):
        global databaseObject
        global newTradesheetTable
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM " + newTradesheetTable + " WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades within an interval
    def getShortNetPL(self, startDate, endDate):
        global databaseObject
        global newTradesheetTable
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM " + newTradesheetTable + " WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in an interval
    def getLongTrades(self, startDate, endDate):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT COUNT(*),1 FROM " + newTradesheetTable + " WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in an interval
    def getShortTrades(self, startDate, endDate):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT COUNT(*),1 FROM " + newTradesheetTable + " WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return Net Profit-Loss of Long trades in original table within an interval
    def getRefLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades in original table within an interval
    def getRefShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in original table within an interval
    def getRefLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in original table within an interval
    def getRefShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return Net PL for long trades per individual from original tradesheet within an interval
    def getIndividualLongNetPL(self, startDate, endDate, individualId):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), 1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1 AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryPL)

    # Function to return Net PL for short trades per individual from original tradesheet within an interval
    def getIndividualShortNetPL(self, startDate, endDate, individualId):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), 1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0 AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryPL)

    # Function to reset all ranks to maximum for initialization
    # Not being used
    def initializeRanks(self):
        global databaseObject
        global rankingTable
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        queryCount = "SELECT COUNT(DISTINCT(individual_id)), 1 FROM old_tradesheet_data_table"
        resultCount = databaseObject.Execute(queryCount)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for count, dummy in resultCount:
            for individualId, dummy in resultIndividuals:
                queryInsert = "INSERT INTO " + rankingTable + \
                              " (individual_id, ranking)" \
                              " VALUES" \
                              " (" + str(individualId) + ", " + str(count) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset all performances to minimum for initialization
    # Not being used
    def initializePerformance(self):
        global databaseObject
        global performanceTable
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for individualId, dummy in resultIndividuals:
            queryInsert = "INSERT INTO " + performanceTable + \
                          " (individual_id, performance)" \
                          " VALUES" \
                          " (" + str(individualId) + ", " + str(gv.dummyPerformance) + ")"
            databaseObject.Execute(queryInsert)

    # Not being used
    def resetRanks(self):
        global databaseObject
        global rankingTable
        queryCount = "SELECT COUNT(DISTINCT(individual_id)), 1 FROM old_tradesheet_data_table"
        resultCount = databaseObject.Execute(queryCount)
        for count, dummy in resultCount:
            queryUpdate = "UPDATE " + rankingTable + " SET ranking=" + str(count)
            databaseObject.Execute(queryUpdate)

    # Not being used
    def resetPerformance(self):
        global databaseObject
        global performanceTable
        queryUpdate = "UPDATE " + performanceTable + " SET performance=" + str(gv.dummyPerformance)
        return databaseObject.Execute(queryUpdate)

    def insertRankingWalkforward(self, startDate, endDate, walkforward):
        global databaseObject
        query = "INSERT INTO " + gv.rankingWalkforwardTableBase + \
                " (ranking_walkforward_id, ranking_start_date, ranking_end_date)" \
                " VALUES" \
                " (" + str(walkforward) + ", '" + str(startDate) + "', '" + str(endDate) + "')"
        return databaseObject.Execute(query)


    # Function to insert rank of an individual
    def insertRank(self, individualId, rank, walkforward):
        global databaseObject
        global rankingTable
        queryInsert = "INSERT INTO " + rankingTable + \
                      " (individual_id, ranking, ranking_walkforward_id)" \
                      " VALUES" \
                      " (" + str(individualId) + ", " + str(rank) + ", " + str(walkforward) + ")"
        databaseObject.Execute(queryInsert)

    # Function to insert performance of an individual
    def insertPerformance(self, individualId, performance, walkforward):
        global databaseObject
        global performanceTable
        queryInsert = "INSERT INTO " + performanceTable + \
                      " (individual_id, performance, ranking_walkforward_id)" \
                      " VALUES" \
                      " (" + str(individualId) + ", " + str(performance) + ", " + str(walkforward) + ")"
        return databaseObject.Execute(queryInsert)

    # Function to get ordered individuals from
    def getRankedIndividuals(self, walkforward):
        global databaseObject
        global performanceTable
        query = "SELECT individual_id, 1 FROM " + performanceTable + " WHERE ranking_walkforward_id=" + str(walkforward) + " ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to return asset at month end
    def getAssetMonthly(self, month, year):
        global databaseObject
        global dailyAssetTable
        queryAsset = "SELECT total_asset, 1 FROM " + dailyAssetTable + " WHERE " \
                     "date=(SELECT MAX(date) FROM " + dailyAssetTable + " WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year) + ")"
        return databaseObject.Execute(queryAsset)

    # Function to return maximum and minimum asset in the month
    def getAssetMonthlyMaxMin(self, month, year):
        global databaseObject
        global dailyAssetTable
        queryAsset = "SELECT MAX(total_asset), MIN(total_asset) FROM " + dailyAssetTable + " WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year)
        return databaseObject.Execute(queryAsset)

    # Function to return trades per month
    def getTradesMonthly(self):
        global databaseObject
        global newTradesheetTable
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM " + newTradesheetTable + " GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return trades per month in base tradesheet
    def getRefTradesMonthly(self):
        global databaseObject
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return Long NetPL and Long trades per month
    def getNetPLLongMonthly(self):
        global databaseObject
        global newTradesheetTable
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM " + newTradesheetTable + \
                  " WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month
    def getNetPLShortMonthly(self):
        global databaseObject
        global newTradesheetTable
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM " + newTradesheetTable + \
                  " WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Long NetPL and Long trades per month in base tradesheet
    def getRefNetPLLongMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month in base tradesheet
    def getRefNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to delete all non-recent entries from qMatrixTable every walk-forward
    def updateQMatrixTableWalkForward(self):
        global databaseObject
        global latestIndividualTable
        global qMatrixTable
        queryUpdate = "DELETE FROM " + qMatrixTable + " WHERE individual_id NOT IN (SELECT individual_id FROM " + latestIndividualTable + " )"
        databaseObject.Execute(queryUpdate)

    # Function to reset latest_individual_table every walk-forward
    def resetLatestIndividualsWalkForward(self):
        global databaseObject
        global latestIndividualTable
        queryReset = "DELETE FROM " + latestIndividualTable
        databaseObject.Execute(queryReset)

    # Function to insert individual id in latest_individual_table every walk-forward
    def insertLatestIndividual(self, individualId):
        global databaseObject
        global latestIndividualTable
        queryCheck = "SELECT EXISTS (SELECT 1 FROM " + latestIndividualTable + " WHERE individual_id=" + str(individualId) + "), 0"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==0:
                queryInsert = "INSERT INTO " + latestIndividualTable + \
                              " (individual_id)" \
                              " VALUES" \
                              " (" + str(individualId) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset assetTable every walk-forward
    def updateAssetWalkForward(self):
        global databaseObject
        global latestIndividualTable
        global assetTable
        queryUpdate = "DELETE FROM " + assetTable + " WHERE individual_id NOT IN ( SELECT individual_id FROM " + \
                      latestIndividualTable + " ) AND individual_id<>" + str(gv.dummyIndividualId)
        databaseObject.Execute(queryUpdate)

    # Function to reset trainingAssetTable every training period
    def resetAssetTraining(self):
        global databaseObject
        global trainingAssetTable
        databaseObject.Execute("DELETE FROM " + trainingAssetTable)
        databaseObject.Execute("INSERT INTO " + trainingAssetTable +
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    def checkQMatrix(self, individualId):
        global databaseObject
        global latestIndividualTable
        query = "SELECT EXISTS( SELECT 1 FROM " + latestIndividualTable + " WHERE individual_id=" + str(individualId) + " ), 1"
        return databaseObject.Execute(query)
