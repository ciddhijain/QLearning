__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

class Live:

    def live(self,  startDate, endDate, walkforward, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject):
        date = startDate
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)
        endTime = timedelta(hours=10, minutes=30)
        dayEndTime = timedelta(hours=15, minutes=30)
        lastCheckedTime = timedelta(hours=9, minutes=15)
        done = False
        logging.info('Starting Live from ' + str(date) + ' to ' + str(endDate))

        while (not done):
            resultTradingDay = dbObject.checkTradingDay(date)
            for checkTradingDay, dummy0 in resultTradingDay:
                if checkTradingDay==1:
                    # Its a trading day. We now get trades from original tradesheet
                    resultTrades = dbObject.getRankedTradesOrdered(date, startTime, endTime, walkforward)
                    for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
                        resultTradesExit = dbObject.getTradesExit(date, lastCheckedTime, entryTime)
                        for id, type, qty, entry_price, exit_price in resultTradesExit:
                            # Exiting Trades
                            freedAsset = 0
                            if type==1:
                                freedAsset = qty*exit_price*(-1)
                            else:
                                freedAsset = qty*(2*entry_price - exit_price)*(-1)
                            dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)
                            dbObject.updateIndividualAsset(id, freedAsset)
                        lastCheckedTime = entryTime
                        resultAvailable = dbObject.getFreeAsset(gv.dummyIndividualId)
                        usedAsset = entryQty*entryPrice
                        for freeAssetTotal, dummy1 in resultAvailable:
                            if float(freeAssetTotal)>=usedAsset:
                                # Overall asset is available
                                resultExists = dbObject.checkIndividualAssetExists(individualId)
                                for exists, dummy2 in resultExists:
                                    if exists==0:
                                        # Individual does not exist in asset table yet. Adding it
                                        dbObject.addIndividualAsset(individualId, usedAsset)
                                        # Taking this trade
                                        dbObject.insertNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                        dbObject.updateIndividualAsset(gv.dummyIndividualId, usedAsset)
                                        dbObject.insertLatestIndividual(individualId)
                                        dbObject.addNewState(individualId, entryDate, entryTime, 1)
                                    else:
                                        # Individual exists already
                                        resultFreeAsset = dbObject.getFreeAsset(individualId)
                                        for freeAsset, dummy3 in resultFreeAsset:
                                            if freeAsset>=usedAsset:
                                               # Individual Asset is available. Taking this trade
                                                dbObject.insertNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                                dbObject.updateIndividualAsset(gv.dummyIndividualId, usedAsset)
                                                dbObject.updateIndividualAsset(individualId, usedAsset)
                                                dbObject.insertLatestIndividual(individualId)
                    resultIndividuals = dbObject.getIndividuals(date, startTime, date, endTime)
                    for individualId, dummy in resultIndividuals:
                        # Calculating mtm
                        mtm = mtmObject.calculateNetPLTrades(individualId, date, startTime, date, endTime, dbObject)
                        # Calculating reward matrix
                        rewardMatrix = rewardMatrixObject.computeRM(mtm)
                        # Calculating q matrix
                        qMatrixObject.calculateQMatrix(rewardMatrix, individualId, dbObject)
                    # Reallocating asset for  individuals
                    reallocationObject.reallocate(date, startTime, date, endTime, dbObject)

                    # Checking if done for the day yet
                    if endTime<dayEndTime:
                        startTime = endTime
                        endTime = endTime + timedelta(hours=gv.hourWindow)

                    else:
                        # Fetching trades that are to exit by the day end
                        resultTradesExit = dbObject.getTradesExitEnd(date, lastCheckedTime, endTime)
                        for id, type, qty, entry_price, exit_price in resultTradesExit:
                            freedAsset = 0
                            if type==1:
                                freedAsset = qty*exit_price*(-1)            # Long Trade
                            else:
                                freedAsset = qty*(2*entry_price - exit_price)*(-1)          # Short Trade
                            dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)
                            dbObject.updateIndividualAsset(id, freedAsset)
                        dbObject.insertDailyAsset(date, endTime)
                        # Checking if we have reached the end of testing period
                        if(date>=periodEndDate):
                            done = True

                        # Going to next day
                        else:
                            date = date + timedelta(days=1)
                            startTime = timedelta(hours=9, minutes=15)
                            endTime = timedelta(hours=10, minutes=30)
                            lastCheckedTime = timedelta(hours=9, minutes=15)
                else:
                    date = date + timedelta(days=1)
                    if(date>periodEndDate):
                        done = True