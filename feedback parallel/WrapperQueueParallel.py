__author__ = 'Ciddhi'

import threading
import time
from datetime import timedelta, datetime
from Reallocation import *
from MTM import *
from QMatrix import *
from PerformanceMeasures import *
import Queue

exitFlag = 0

class ParallelWrapper(threading.Thread):
    def __init__(self, threadName, q):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.q = q

    def run(self):
        print("Starting : " + str(self.threadName))
        processIndividual(self.q)
        print("Exiting : " + str(self.threadName))

def processIndividual(q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            individualId = data[0]
            date = data[1]
            startTime = data[2]
            endTime = data[3]
            mtmObject = MTM()
            rewardMatrixObject = RewardMatrix()
            qMatrixObject = QMatrix()
            dbObject = DBUtils()
            dbObject.dbConnect()
            print('Calculating mtm for all trades of this individual')
            mtmObject.calculateMTM(individualId, gv.aggregationUnit, date, startTime, date, endTime, dbObject)
            print('Calculating reward matrix for these individuals')
            rewardMatrix = rewardMatrixObject.computeRM(individualId, date, startTime, date, endTime, dbObject)
            print('Calculating q matrix for these individuals')
            qMatrixObject.calculateQMatrix(rewardMatrix, individualId, dbObject)
            queueLock.release()
            #dbObject.dbClose()
        else:
            queueLock.release()

if __name__ == "__main__":
    dbObject = DBUtils()
    reallocationObject = Reallocation()
    performanceObject = PerformanceMeasures()

    dbObject.dbConnect()

    print('Started at : ' + str(datetime.now()))

    dbObject.dbQuery("DELETE FROM asset_allocation_table")
    dbObject.dbQuery("DELETE FROM asset_daily_allocation_table")
    dbObject.dbQuery("DELETE FROM mtm_table")
    dbObject.dbQuery("DELETE FROM tradesheet_data_table")
    dbObject.dbQuery("DELETE FROM reallocation_table")
    dbObject.dbQuery("DELETE FROM q_matrix_table")

    date = datetime(2012, 4, 9).date()
    periodEndDate = datetime(2013, 12, 20).date()
    print('date : ' + str(date))
    startTime = timedelta(hours=9, minutes=15)
    endTime = timedelta(hours=10, minutes=30)
    dayEndTime = timedelta(hours=15, minutes=30)
    lastCheckedTime = timedelta(hours=9, minutes=15)
    print('Resetting asset allocation table')
    dbObject.resetAssetAllocation(date, startTime)
    done = False

    while (not done):

        resultTradingDay = dbObject.checkTradingDay(date)
        for checkTradingDay, dummy0 in resultTradingDay:
            if checkTradingDay==1:
                print('Getting trades from superset')
                resultTrades = dbObject.getTradesOrdered(date, startTime, endTime)

                print('For all trades received')
                for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:

                    print('Fetching trades that are to exit')
                    resultTradesExit = dbObject.getTradesExit(date, lastCheckedTime, entryTime)
                    for id, type, qty, entry_price, exit_price in resultTradesExit:
                        freedAsset = 0
                        if type==0:
                            freedAsset = qty*exit_price*(-1)
                        else:
                            freedAsset = qty*(2*entry_price - exit_price)*(-1)
                        dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)
                        dbObject.updateIndividualAsset(id, freedAsset)
                    lastCheckedTime = entryTime

                    print('Fetching asset available')
                    resultAvailable = dbObject.getFreeAsset(gv.dummyIndividualId)
                    print('Asset needed for this trade = ')
                    usedAsset = entryQty*entryPrice
                    print(usedAsset)
                    for freeAssetTotal, dummy1 in resultAvailable:
                        print('Checking if asset is available for this amount')
                        if float(freeAssetTotal)>=usedAsset:
                            print('Checking if this individual is in asset_allocation_table')
                            resultExists = dbObject.checkIndividualAssetExists(individualId)
                            for exists, dummy2 in resultExists:
                                if exists==0:
                                    print('As this individual does not exist, add it to asset_allocation_table and reallocation_table')
                                    dbObject.addIndividualAsset(individualId, usedAsset)
                                    dbObject.addNewState(individualId, date, entryTime, 1)
                                    print('And adding the trade to new tradesheet')
                                    dbObject.insertNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                    dbObject.updateIndividualAsset(gv.dummyIndividualId, usedAsset)
                                else:
                                    print('Checking if this individual has asset for this new trade')
                                    resultFreeAsset = dbObject.getFreeAsset(individualId)
                                    for freeAsset, dummy3 in resultFreeAsset:
                                        if freeAsset>=usedAsset:
                                            print('Since the individual has asset, adding the trade to new tradesheet')
                                            dbObject.insertNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                                            dbObject.updateIndividualAsset(gv.dummyIndividualId, usedAsset)
                                            dbObject.updateIndividualAsset(individualId, usedAsset)
                print('Fetching individuals which took trades in last hour')
                resultIndividuals = dbObject.getIndividuals(date, startTime, date, endTime)
                exitFlag = 0
                threads = []
                threadList = []
                threadNum = 1
                for i in range(gv.maxThreads):
                    threadList.append("Thread" + str(threadNum))
                    threadNum += 1

                queueLock = threading.Lock()
                workQueue = Queue.Queue(0)

                queueLock.acquire()
                for individual_id, dummy4 in resultIndividuals:
                    workQueue.put((individual_id, date, startTime, endTime))
                queueLock.release()

                for tName in threadList:
                    threadObj = ParallelWrapper(tName, workQueue)
                    threadObj.start()
                    threads.append(threadObj)

                while not workQueue.empty():
                    pass
                exitFlag = 1

                for t in threads:
                    t.join()

                print "Done Calculating MTM, Q matrices -----------------------"

                print('Reallocating asset for these individuals')
                reallocationObject.reallocate(date, startTime, date, endTime, dbObject)

                if endTime<dayEndTime:
                    startTime = endTime
                    endTime = endTime + timedelta(hours=gv.hourWindow)
                    print('Not yet done for the day : ' + str(date))
                    print('New start time : ' + str(startTime))
                    print('New end time : ' + str(endTime))
                else:
                    print('Fetching trades that are to exit by the day end')
                    resultTradesExit = dbObject.getTradesExitEnd(date, lastCheckedTime, endTime)
                    for id, type, qty, entry_price, exit_price in resultTradesExit:
                        freedAsset = 0
                        if type==0:
                            freedAsset = qty*exit_price*(-1)
                        else:
                            freedAsset = qty*(2*entry_price - exit_price)*(-1)
                        dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)
                        dbObject.updateIndividualAsset(id, freedAsset)

                    dbObject.insertDailyAsset(date, endTime)

                    print('Checking if we have reached the end of testing period')
                    if(date>=periodEndDate):
                        done = True
                    else:
                        date = date + timedelta(days=1)
                        startTime = timedelta(hours=9, minutes=15)
                        endTime = timedelta(hours=10, minutes=30)
                        lastCheckedTime = timedelta(hours=9, minutes=15)
                        print('Going to next day')

                        print('\n')
                        print('------------------------------------------------------------------------------------------------------')
                        print('\n')
                        print(datetime.now())
                        print('New day : ' + str(date))
                        print('New start time : ' + str(startTime))
                        print('New end time : ' + str(endTime))
            else:
                date = date + timedelta(days=1)
                if(date>periodEndDate):
                    done = True

    performanceObject.CalculateTradesheetPerformanceMeasures(date, periodEndDate, dbObject)
    dbObject.dbClose()
    print('Finished at : ' + str(datetime.now()))

