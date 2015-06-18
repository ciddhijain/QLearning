__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
from multiprocessing import Lock, Pool
from DBUtils import *
import logging

class Training:

    def train(self, startDate, endDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject):

        date = startDate
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)
        endTime = timedelta(hours=10, minutes=30)
        dayEndTime = timedelta(hours=15, minutes=30)
        lastCheckedTime = timedelta(hours=9, minutes=15)
        done = False
        logging.info('Starting Training from ' + str(date) + ' to ' + str(endDate))

        while (not done):
            resultTradingDay = dbObject.checkTradingDay(date)
            for checkTradingDay, dummy0 in resultTradingDay:
                if checkTradingDay==1:
                    # Its a trading day
                    resultTrades = dbObject.getRankedTradesOrdered(date, startTime, endTime)
                    for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
                        resultTradesExit = dbObject.getTrainingTradesExit(date, lastCheckedTime, entryTime)
                        for id, type, qty, entry_price, exit_price in resultTradesExit:
                            # Exiting Trades
                            freedAsset = 0
                            if type==1:
                                freedAsset = qty*exit_price*(-1)
                            else:
                                freedAsset = qty*(2*entry_price - exit_price)*(-1)
                            dbObject.updateTrainingIndividualAsset(gv.dummyIndividualId, freedAsset)
                            dbObject.updateTrainingIndividualAsset(id, freedAsset)
                        lastCheckedTime = entryTime
                        resultAvailable = dbObject.getTrainingFreeAsset(gv.dummyIndividualId)
                        usedAsset = entryQty*entryPrice
                        for freeAssetTotal, dummy1 in resultAvailable:
                            if float(freeAssetTotal)>=usedAsset:
                                # Overall asset is available
                                resultExists = dbObject.checkTrainingIndividualAssetExists(individualId)
                                for exists, dummy2 in resultExists:
                                    if exists==0:
                                        # Individual does not exist in asset table yet. Adding it
                                        dbObject.addTrainingIndividualAsset(individualId, usedAsset)
                                        # Taking this trade
                                        dbObject.insertTrainingNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                        dbObject.updateTrainingIndividualAsset(gv.dummyIndividualId, usedAsset)
                                    else:
                                        # Individual exists already
                                        resultFreeAsset = dbObject.getTrainingFreeAsset(individualId)
                                        for freeAsset, dummy3 in resultFreeAsset:
                                            if freeAsset>=usedAsset:
                                                # Individual Asset is available. Taking this trade
                                                dbObject.insertTrainingNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                                dbObject.updateTrainingIndividualAsset(gv.dummyIndividualId, usedAsset)
                                                dbObject.updateTrainingIndividualAsset(individualId, usedAsset)
                    resultIndividuals = dbObject.getTrainingIndividuals(date, startTime, date, endTime)

                    workList = []
                    for individualId, dummy in resultIndividuals:
                        resultCheck = dbObject.checkQMatrix(individualId)
                        for check, dummy4 in resultCheck:
                            if check==0:
                                mtmObject.calculateTrainingMTM(individualId, date, startTime, date, endTime, dbObject)
                                # Calculating reward matrix
                                rewardMatrix = rewardMatrixObject.computeTrainingRM(individualId, date, startTime, date, endTime, dbObject)
                                # Calculating q matrix
                                qMatrixObject.calculateQMatrix(rewardMatrix, individualId, dbObject)

                    # Checking if we have reached end of the day
                    if endTime<dayEndTime:
                        startTime = endTime
                        endTime = endTime + timedelta(hours=gv.hourWindow)
                    else:
                        # Fetching trades that are to exit by the day end
                        resultTradesExit = dbObject.getTrainingTradesExitEnd(date, lastCheckedTime, endTime)
                        for id, type, qty, entry_price, exit_price in resultTradesExit:
                            freedAsset = 0
                            if type==1:
                                freedAsset = qty*exit_price*(-1)            # Long Trade
                            else:
                                freedAsset = qty*(2*entry_price - exit_price)*(-1)          # Short Trade
                            dbObject.updateTrainingIndividualAsset(gv.dummyIndividualId, freedAsset)
                            dbObject.updateTrainingIndividualAsset(id, freedAsset)
                        # Checking if we have reached the end of testing period
                        if(date>=periodEndDate):
                            done = True
                        else:
                            # Going to next day
                            date = date + timedelta(days=1)
                            startTime = timedelta(hours=9, minutes=15)
                            endTime = timedelta(hours=10, minutes=30)
                            lastCheckedTime = timedelta(hours=9, minutes=15)
                else:
                    date = date + timedelta(days=1)
                    if(date>periodEndDate):
                        done = True
        logging.info('Done Training')