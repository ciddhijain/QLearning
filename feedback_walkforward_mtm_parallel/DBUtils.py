__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from decimal import Decimal

class DBUtils:

    databaseObject = None

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
        queryInsertTrade = "INSERT INTO tradesheet_data_table" \
                           " (trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time, exit_price)" \
                           " VALUES" \
                           " (" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + ", '" + str(entryDate) + "', '" + str(entryTime) +\
                           "', " + str(entryPrice) + ", " + str(entryQty) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ")"
        #print(queryInsertTrade)
        databaseObject.Execute(queryInsertTrade)

    # Function to insert new trade in training_tradesheet
    def insertTrainingNewTrade(self, tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice):
        global databaseObject
        queryInsertTrade = "INSERT INTO training_tradesheet_data_table" \
                           " (trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time, exit_price)" \
                           " VALUES" \
                           " (" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + ", '" + str(entryDate) + "', '" + str(entryTime) +\
                           "', " + str(entryPrice) + ", " + str(entryQty) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ")"
        #print(queryInsertTrade)
        databaseObject.Execute(queryInsertTrade)

    # Function to get individuals which have active trades in a given interval of time on a given day
    def getIndividuals (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM tradesheet_data_table WHERE entry_time<'" + str(endTime) + \
                           "' AND exit_time>'" + str(startTime) + "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryIndividuals)

    # Function to get individuals which have active trades in a given interval of time on a given day during training
    def getTrainingIndividuals (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM training_tradesheet_data_table WHERE entry_time<'" + str(endTime) + \
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
        queryTrades = "SELECT trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time " \
                      "FROM tradesheet_data_table WHERE entry_time<='" + str(endTime) + "' AND exit_time>='" + str(startTime) + \
                      "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get new trades from original tradesheet
    def getTradesOrdered (self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "' AND entry_time<'" + str(endTime) + \
                      "' AND entry_time>='" + str(startTime) + "' ORDER BY entry_time"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    # Function to get new trades from original tradesheet based on ranking
    def getRankedTradesOrdered (self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT t.* FROM old_tradesheet_data_table AS t JOIN ranking_table as r ON t.individual_id=r.individual_id" \
                      " WHERE t.entry_date='" + str(date) + "' AND t.entry_time<'" + str(endTime) + "' AND t.entry_time>='" + str(startTime) + \
                      "' ORDER BY t.entry_time, r.ranking"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    # Function to get trades taken by an individual in an interval
    def getTradesIndividual(self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM tradesheet_data_table WHERE entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + \
                      "' AND exit_time>='" + str(startTime) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades taken by an individual in an interval during training
    def getTrainingTradesIndividual(self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM training_tradesheet_data_table WHERE entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + \
                      "' AND exit_time>='" + str(startTime) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval
    def getTradesExit(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end
    def getTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval during training
    def getTrainingTradesExit(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM training_tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end during training
    def getTrainingTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM training_tradesheet_data_table WHERE exit_date='" + str(date) + \
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

    # Function to insert MTM value in db
    def insertMTM(self, individualId, tradeId, tradeType, entryDate, mtmTime, mtm):
        global databaseObject
        queryCheckRecord = "SELECT EXISTS (SELECT 1 FROM mtm_table WHERE trade_id=" + str(tradeId) + " AND date='" + str(entryDate) + \
                           "' AND time='" + str(mtmTime) + "'), 0"

        resultRecord = databaseObject.Execute(queryCheckRecord)
        for result, dummy in resultRecord:
            if result==0:
                queryInsertMTM = "INSERT INTO mtm_table " \
                                 "(trade_id, individual_id, trade_type, date, time, mtm) " \
                                 "VALUES " \
                                 "(" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + \
                                 ", '" + str(entryDate) + "', '" + str(mtmTime) + "', " + str(mtm) + ")"
                return databaseObject.Execute(queryInsertMTM)

    # Function to insert MTM value in db during training
    def insertTrainingMTM(self, individualId, tradeId, tradeType, entryDate, mtmTime, mtm):
        global databaseObject
        queryCheckRecord = "SELECT EXISTS (SELECT 1 FROM training_mtm_table WHERE trade_id=" + str(tradeId) + " AND date='" + str(entryDate) + \
                           "' AND time='" + str(mtmTime) + "'), 0"

        resultRecord = databaseObject.Execute(queryCheckRecord)
        for result, dummy in resultRecord:
            if result==0:
                queryInsertMTM = "INSERT INTO training_mtm_table " \
                                 "(trade_id, individual_id, trade_type, date, time, mtm) " \
                                 "VALUES " \
                                 "(" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + \
                                 ", '" + str(entryDate) + "', '" + str(mtmTime) + "', " + str(mtm) + ")"
                return databaseObject.Execute(queryInsertMTM)

    # Function to get last reallocation date for an individual
    # Not being used currently
    def getStartDate (self, individualId):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table WHERE individual_id=" + str(individualId)
        global databaseObject
        return databaseObject.Execute(queryDate)

    # Function to get last reallocation time for an individual
    # Not being used currently
    def getStartTime (self, individualId):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                    "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + ")"
        global databaseObject
        return databaseObject.Execute(queryTime)

    # Function to get last reallocation time overall
    # Not being used currently
    def getLastReallocationTime(self):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table)"
        global databaseObject
        return databaseObject.Execute(queryTime)

    # Function to get last reallocation date overall
    # Not being used currently
    def getLastReallocationDate(self):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table"
        global databaseObject
        return databaseObject.Execute(queryDate)

    # Not being used currently
    def updateStartTime(self, individualId, startDate, startTime):
        global databaseObject
        queryUpdate = "UPDATE reallocation_table SET last_reallocation_date='" + str(startDate) + \
                      "', last_reallocation_time=" + str(startTime) + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryUpdate)

    # Function to get net MTM for all long trades
    def getTotalPosMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) +\
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # function to get total quantity for all long trades
    def getTotalPosQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all short trades
    def getTotalNegMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) + \
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # Function to get total quantity for all short trades
    def getTotalNegQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all long trades during training
    def getTrainingTotalPosMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM training_mtm_table WHERE individual_id=" + str(individualId) +\
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # function to get total quantity for all long trades during training
    def getTrainingTotalPosQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM training_tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all short trades during training
    def getTrainingTotalNegMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM training_mtm_table WHERE individual_id=" + str(individualId) + \
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # Function to get total quantity for all short trades during training
    def getTrainingTotalNegQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM training_tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get Q Matrix of an individual
    def getQMatrix (self, individualId):
        global databaseObject
        queryQM = "SELECT row_num, column_num, q_value FROM q_matrix_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryQM)

    # Function to insert / update Q matrix of an individual
    def updateQMatrix(self, individualId, qm):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + "), 1"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==1:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryUpdate = "UPDATE q_matrix_table SET q_value=" + str(round(qm[i,j], 10)) + " WHERE individual_id=" + str(individualId) + \
                                      " AND row_num=" + str(i) + " AND column_num=" + str(j)
                        databaseObject.Execute(queryUpdate)
            else:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryInsert = "INSERT INTO q_matrix_table " \
                                     "(individual_id, row_num, column_num, q_value)" \
                                     " VALUES " \
                                     "(" + str(individualId) + ", " + str(i) + ", " + str(j) + ", " + str(round(qm[i,j], 10)) + ")"
                        databaseObject.Execute(queryInsert)

    # Function to insert individual entry in asset_allocation_table
    def addIndividualAsset (self, individualId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO asset_allocation_table" \
                        "(individual_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(individualId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert individual entry in training_asset_allocation_table
    def addTrainingIndividualAsset (self, individualId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO training_asset_allocation_table" \
                        "(individual_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(individualId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to check if an individual's entry exists in asset_allocation_table
    def checkIndividualAssetExists (self, individualId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in training_asset_allocation_table
    def checkTrainingIndividualAssetExists (self, individualId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM training_asset_allocation_table WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to update individual's asset
    def updateIndividualAsset(self, individualId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM asset_allocation_table WHERE individual_id=" + str(individualId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE individual_id=" + str(individualId)
            return databaseObject.Execute(queryUpdate)

    # Function to update individual's asset during training
    def updateTrainingIndividualAsset(self, individualId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM training_asset_allocation_table WHERE individual_id=" + str(individualId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE training_asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE individual_id=" + str(individualId)
            return databaseObject.Execute(queryUpdate)

    # Function to get the asset being used by an individual at a given time
    # Not used currently
    def getUsedAsset (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryUsedAsset = "SELECT entry_qty*entry_price, 1 FROM tradesheet_data_table WHERE individual_id=" + str(individualId) + \
                         " AND entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + "' AND exit_time>'" + str(endTime) + "'"
        return databaseObject.Execute(queryUsedAsset)

    # Function to add individual's entry in reallocation table
    def addNewState(self, individualId, date, time, state):
        global databaseObject
        queryNewState = "INSERT INTO reallocation_table" \
                        " (individual_id, last_reallocation_date, last_reallocation_time, last_state)" \
                        " VALUES" \
                        " (" + str(individualId) + ", '" + str(date) + "', '" + str(time) + "', " + str(state) + ")"
        return databaseObject.Execute(queryNewState)

    # Function to get last state for an individual
    def getLastState (self, individualId):
        global databaseObject
        queryLastState = "SELECT last_state, individual_id FROM reallocation_table WHERE individual_id=" + str(individualId) + \
                         " AND last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE " \
                         "individual_id=" + str(individualId) + ") AND last_reallocation_time=(SELECT MAX(last_reallocation_time) " \
                        "FROM reallocation_table WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                        "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + "))"
        return databaseObject.Execute(queryLastState)

    # Function to get next state for an individual
    def getNextState (self, individualId, currentState):
        global databaseObject
        '''
        queryNextState = "SELECT column_num, individual_id FROM q_matrix_table WHERE individual_id=" + str(individualId) \
                         + " AND row_num=" + str(currentState) + \
                         " AND q_value=(SELECT MAX(q_value) FROM q_matrix_table WHERE individual_id=" \
                         + str(individualId) + " AND row_num=" + str(currentState) + ")"
        '''
        queryMaxQValue = "SELECT MAX(q_value), 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + " AND row_num=" + str(currentState)
        resultMaxQValue = databaseObject.Execute(queryMaxQValue)
        queryCurrentQValue = "SELECT q_value, 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + " AND row_num=" + str(currentState) + \
                             " AND column_num=1"
        resultCurrentQValue = databaseObject.Execute(queryCurrentQValue)
        for maxQValue, dummy1 in resultMaxQValue:
            for currentQValue, dummy2 in resultCurrentQValue:
                # Checking with help of percentage difference between the maximum and current Q value
                if currentQValue!=0:
                    diff = float(abs(maxQValue-currentQValue)/currentQValue*100)
                    if diff>gv.zeroRange:
                        queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT MAX(q_value) FROM q_matrix_table WHERE individual_id=" + \
                                         str(individualId) + " AND row_num=" + str(currentState) + ")"
                        return databaseObject.Execute(queryNextState)
                    else:
                        queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT q_value FROM q_matrix_table WHERE individual_id=" + str(individualId) + \
                                         " AND row_num=" + str(currentState) + " AND column_num=1)"
                        return databaseObject.Execute(queryNextState)
                else:
                    queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE individual_id=" + str(individualId) + " AND row_num=" + \
                                     str(currentState) + " AND column_num=1"
                    return databaseObject.Execute(queryNextState)

    # Function to reduce free asset for an individual
    def reduceFreeAsset(self, individualId, unitQty):
        global databaseObject
        resultCurrentFreeAsset = databaseObject.Execute("SELECT free_asset, total_asset FROM asset_allocation_table "
                                                        "WHERE individual_id="+str(individualId))
        for freeAsset, totalAsset in resultCurrentFreeAsset:
            if (float(freeAsset)>=unitQty):
                newFreeAsset = float(freeAsset) - unitQty
                newTotalAsset = float(totalAsset) - unitQty
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = float(totalAsset - freeAsset)
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=0, total_asset=" + str(round(newTotalAsset,4)) + \
                              " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)

    # Function to increase free asset for an individual
    def increaseFreeAsset(self, individualId, unitQty):
        global databaseObject
        resultCurrentTotalAsset = databaseObject.Execute("SELECT total_asset, free_asset FROM asset_allocation_table"
                                                        " WHERE individual_id=" + str(individualId))
        for totalAsset, freeAsset in resultCurrentTotalAsset:
            newTotalAsset = float(totalAsset) + unitQty
            newFreeAsset = float(freeAsset) + unitQty
            if newTotalAsset<=gv.maxAsset:
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                #print(queryUpdate)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = gv.maxAsset
                newFreeAsset = float(freeAsset) + gv.maxAsset - float(totalAsset)
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                #print(queryUpdate)
                return databaseObject.Execute(queryUpdate)

    # Function to get current free asset for an individual
    def getFreeAsset(self, individualId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)

    # Function to get current free asset for an individual during training
    def getTrainingFreeAsset(self, individualId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM training_asset_allocation_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)

    # Function to reset asset_allocation_table at the beginning
    def resetAssetAllocation(self, date, time):
        global databaseObject
        #databaseObject.Execute("DELETE FROM asset_allocation_table")
        databaseObject.Execute("INSERT INTO asset_allocation_table"
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.maxTotalAsset,4)) + ", 0, " + str(round(gv.maxTotalAsset,4)) + ")")
        databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                               " (date, time, total_asset)"
                               " VALUES"
                               " ('" + str(date) + "', '" + str(time) + "', " + str(round(gv.maxTotalAsset, 4)) + ")")
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to insert free asset at day end into asset_daily_allocation_table
    def insertDailyAsset(self, date, time):
        global databaseObject
        resultAsset = databaseObject.Execute("SELECT free_asset, 1 from asset_allocation_table where individual_id=" + str(gv.dummyIndividualId))
        for totalAsset, dummy in resultAsset:
            databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                                   " (date, time, total_asset)"
                                   " VALUES"
                                   " ('" + str(date) + "', '" + str(time) + "', " + str(totalAsset) + ")")

    # Function to return Net Profit-Loss of Long trades within an interval
    def getLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades within an interval
    def getShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in an interval
    def getLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in an interval
    def getShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
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

    # Function to return Net Profit-Loss of Long trades in original table within an interval
    def getTrainingLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM training_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades in original table within an interval
    def getTrainingShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM training_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in original table within an interval
    def getTrainingLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM training_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in original table within an interval
    def getTrainingShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM training_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
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

    # Function to get Drawdown for an individual in an interval
    def getIndividualDrawdown(self, startDate, endDate, individualId):
        global databaseObject
        queryDD = ""
        return None

    # Function to reset all ranks to maximum for initialization
    def initializeRanks(self):
        global databaseObject
        #databaseObject.Execute("DELETE FROM ranking_table")
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        queryCount = "SELECT COUNT(DISTINCT(individual_id)), 1 FROM old_tradesheet_data_table"
        resultCount = databaseObject.Execute(queryCount)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for count, dummy in resultCount:
            for individualId, dummy in resultIndividuals:
                queryInsert = "INSERT INTO ranking_table" \
                              " (individual_id, ranking)" \
                              " VALUES" \
                              " (" + str(individualId) + ", " + str(count) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset all performances to minimum for initialization
    def initializePerformance(self):
        global databaseObject
        #databaseObject.Execute("DELETE FROM ranking_table")
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for individualId, dummy in resultIndividuals:
            queryInsert = "INSERT INTO performance_table" \
                          " (individual_id, performance)" \
                          " VALUES" \
                          " (" + str(individualId) + ", " + str(gv.dummyPerformance) + ")"
            databaseObject.Execute(queryInsert)

    def resetRanks(self):
        global databaseObject
        queryCount = "SELECT COUNT(DISTINCT(individual_id)), 1 FROM old_tradesheet_data_table"
        resultCount = databaseObject.Execute(queryCount)
        for count, dummy in resultCount:
            queryUpdate = "UPDATE ranking_table SET ranking=" + str(count)
            databaseObject.Execute(queryUpdate)

    def resetPerformance(self):
        global databaseObject
        queryUpdate = "UPDATE performance_table SET performance=" + str(gv.dummyPerformance)
        return databaseObject.Execute(queryUpdate)

    # Function to update rank of an individual
    def updateRank(self, individualId, rank):
        global databaseObject
        queryUpdate = "UPDATE ranking_table SET ranking=" + str(rank) + " WHERE individual_id=" + str(individualId)
        databaseObject.Execute(queryUpdate)

    # Function to update performance of an individual
    def updatePerformance(self, individualId, performance):
        global databaseObject
        queryUpdate = "UPDATE performance_table SET performance=" + str(performance) + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryUpdate)

    # FUnction to get ordered individuals from
    def getRankedIndividuals(self):
        global databaseObject
        query = "SELECT individual_id, 1 FROM performance_table WHERE NOT performance=" + str(gv.dummyPerformance) + " ORDER BY performance DESC"
        return databaseObject.Execute(query)

    '''
    # Function to return trades per individual from original tradesheet within an interval
    def getIndividualTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT individual_id, COUNT(*) FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='"\
                      + str(endDate) + "' GROUP BY individual_id"
        return databaseObject.Execute(queryTrades)

    '''

    # Function to return asset at month end
    def getAssetMonthly(self, month, year):
        global databaseObject
        queryAsset = "SELECT total_asset, 1 FROM asset_daily_allocation_table WHERE " \
                     "date=(SELECT MAX(date) FROM asset_daily_allocation_table WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year) + ")"
        return databaseObject.Execute(queryAsset)

    # Function to return maximum and minimum asset in the month
    def getAssetMonthlyMaxMin(self, month, year):
        global databaseObject
        queryAsset = "SELECT MAX(total_asset), MIN(total_asset) FROM asset_daily_allocation_table WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year)
        return databaseObject.Execute(queryAsset)

    # Function to return trades per month
    def getTradesMonthly(self):
        global databaseObject
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return trades per month in base tradesheet
    def getRefTradesMonthly(self):
        global databaseObject
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return Long NetPL and Long trades per month
    def getNetPLLongMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month
    def getNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
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

    # Function to delete all non-recent entries from q_matrix_table every walk-forward
    def updateQMatrixTableWalkForward(self):
        global databaseObject
        queryUpdate = "DELETE FROM q_matrix_table WHERE individual_id NOT IN (SELECT individual_id FROM latest_individual_table)"
        databaseObject.Execute(queryUpdate)

    # Function to reset latest_individual_table every walk-forward
    def resetLatestIndividualsWalkForward(self):
        global databaseObject
        queryReset = "DELETE FROM latest_individual_table"
        databaseObject.Execute(queryReset)

    # Function to insert individual id in latest_individual_table every walk-forward
    def insertLatestIndividual(self, individualId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM latest_individual_table WHERE individual_id=" + str(individualId) + "), 0"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==0:
                queryInsert = "INSERT INTO latest_individual_table" \
                              " (individual_id)" \
                              " VALUES" \
                              " (" + str(individualId) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset asset_allocation_table every walk-forward
    def updateAssetWalkForward(self):
        global databaseObject
        queryUpdate = "DELETE FROM asset_allocation_table WHERE individual_id NOT IN (SELECT individual_id FROM latest_individual_table) AND individual_id<>" + str(gv.dummyIndividualId)
        databaseObject.Execute(queryUpdate)

    # Function to reset training_asset_allocation_table every training period
    def resetAssetTraining(self):
        global databaseObject
        databaseObject.Execute("DELETE FROM training_asset_allocation_table")
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (individual_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(gv.dummyIndividualId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    def checkQMatrix(self, individualId):
        global databaseObject
        query = "SELECT EXISTS( SELECT 1 FROM latest_individual_table WHERE individual_id=" + str(individualId) + " ), 1"
        return databaseObject.Execute(query)