__author__ = 'IIT'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from decimal import Decimal

class DBUtils:

    databaseObject = None

    def dbConnect (self):
        db_username = 'root'
        db_password = 'controljp'
        db_host = '127.0.0.1'
        db_name = 'MKT'
        db_port = '3306'
        global databaseObject
        databaseObject = DatabaseManager(db_username, db_password,db_host,db_port, db_name)
        databaseObject.Connect()

    def dbQuery (self, query):
        global databaseObject
        return databaseObject.Execute(query)

    def dbClose (self):
        global databaseObject
        databaseObject.Close()

    def checkTradingDay(self, date):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "'), 1"
        return databaseObject.Execute(queryCheck)

    def insertNewTrade(self, tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice):
        global databaseObject
        queryInsertTrade = "INSERT INTO tradesheet_data_table" \
                           " (trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time, exit_price)" \
                           " VALUES" \
                           " (" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + ", '" + str(entryDate) + "', '" + str(entryTime) +\
                           "', " + str(entryPrice) + ", " + str(entryQty) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ")"
        #print(queryInsertTrade)
        databaseObject.Execute(queryInsertTrade)

    def getIndividuals (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM tradesheet_data_table WHERE entry_time<'" + str(endTime) + \
                           "' AND exit_time>'" + str(startTime) + "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryIndividuals)

    def getAllIndividuals(self):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        return databaseObject.Execute(queryIndividuals)

    def getTrades (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time " \
                      "FROM tradesheet_data_table WHERE entry_time<='" + str(endTime) + "' AND exit_time>='" + str(startTime) + \
                      "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryTrades)

    def getTradesOrdered (self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "' AND entry_time<'" + str(endTime) + \
                      "' AND entry_time>='" + str(startTime) + "' ORDER BY entry_time"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    def getTradesExit(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    def getTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "'"
        return databaseObject.Execute(queryTrades)

    def getPriceSeries (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryPriceSeries = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time>='" + str(startTime) + \
                           "' AND time<='" + str(endTime) + "'"
        return databaseObject.Execute(queryPriceSeries)

    def getPrice(self, startDate, startTime):
        global databaseObject
        queryPrice = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time='" + str(startTime) + "'"
        return databaseObject.Execute(queryPrice)

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

    def getStartDate (self, individualId):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table WHERE individual_id=" + str(individualId)
        global databaseObject
        return databaseObject.Execute(queryDate)

    def getStartTime (self, individualId):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                    "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + ")"
        global databaseObject
        return databaseObject.Execute(queryTime)

    def getLastReallocationTime(self):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table)"
        global databaseObject
        return databaseObject.Execute(queryTime)

    def getLastReallocationDate(self):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table"
        global databaseObject
        return databaseObject.Execute(queryDate)

    def updateStartTime(self, individualId, startDate, startTime):
        global databaseObject
        queryUpdate = "UPDATE reallocation_table SET last_reallocation_date='" + str(startDate) + \
                      "', last_reallocation_time=" + str(startTime) + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryUpdate)

    def getTotalPosMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) +\
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    def getTotalPosQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    def getTotalNegMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) + \
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    def getTotalNegQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    def getQMatrix (self, individualId):
        global databaseObject
        queryQM = "SELECT row_num, column_num, q_value FROM q_matrix_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryQM)

    def addIndividualAsset (self, individualId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO asset_allocation_table" \
                        "(individual_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(individualId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    def checkIndividualAssetExists (self, individualId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

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

    def getUsedAsset (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryUsedAsset = "SELECT entry_qty*entry_price, 1 FROM tradesheet_data_table WHERE individual_id=" + str(individualId) + \
                         " AND entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + "' AND exit_time>'" + str(endTime) + "'"
        return databaseObject.Execute(queryUsedAsset)

    def addNewState(self, individualId, date, time, state):
        global databaseObject
        queryNewState = "INSERT INTO reallocation_table" \
                        " (individual_id, last_reallocation_date, last_reallocation_time, last_state)" \
                        " VALUES" \
                        " (" + str(individualId) + ", '" + str(date) + "', '" + str(time) + "', " + str(state) + ")"
        return databaseObject.Execute(queryNewState)

    def getLastState (self, individualId):
        global databaseObject
        queryLastState = "SELECT last_state, individual_id FROM reallocation_table WHERE individual_id=" + str(individualId) + \
                         " AND last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE " \
                         "individual_id=" + str(individualId) + ") AND last_reallocation_time=(SELECT MAX(last_reallocation_time) " \
                        "FROM reallocation_table WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                        "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + "))"
        return databaseObject.Execute(queryLastState)

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

    def getFreeAsset(self, individualId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)

    def clearAssetAllocation(self):
        global databaseObject
        databaseObject.Execute("UPDATE asset_allocation_table SET free_asset=30, total_asset=100 WHERE individual_id<>3")
        return databaseObject.Execute("UPDATE asset_allocation_table SET free_asset=0, total_asset=0 WHERE individual_id=3")

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

    def insertDailyAsset(self, date, time):
        global databaseObject
        resultAsset = databaseObject.Execute("SELECT free_asset, 1 from asset_allocation_table where individual_id=" + str(gv.dummyIndividualId))
        for totalAsset, dummy in resultAsset:
            databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                                   " (date, time, total_asset)"
                                   " VALUES"
                                   " ('" + str(date) + "', '" + str(time) + "', " + str(totalAsset) + ")")

    def clearReallocation(self):
        global databaseObject
        return databaseObject.Execute("DELETE FROM reallocation_table WHERE last_reallocation_time>915")

    # Function to return Net Profit-Loss of Long trades within an interval
    def getLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades within an interval
    def getShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in an interval
    def getLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in an interval
    def getShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return Net Profit-Loss of Long trades in original table within an interval
    def getRefLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades in original table within an interval
    def getRefShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in original table within an interval
    def getRefLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in original table within an interval
    def getRefShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

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
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month
    def getNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Long NetPL and Long trades per month in base tradesheet
    def getRefNetPLLongMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month in base tradesheet
    def getRefNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)