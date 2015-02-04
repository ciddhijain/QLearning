__author__ = 'Ciddhi'

from DBUtils import *
from datetime import timedelta, datetime

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()

    date = datetime(2012, 4, 9).date()
    periodEndDate = datetime(2012, 7, 9).date()
    startTime = timedelta(hours=9, minutes=15)
    endTime = timedelta(hours=10, minutes=30)
    dayEndTime = timedelta(hours=15, minutes=30)
    lastCheckedTime = timedelta(hours=9, minutes=15)
    dbObject.resetAssetAllocation()
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
                    for id, qty, price in resultTradesExit:
                        freedAsset = qty*price*(-1)
                        dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)
                    lastCheckedTime = entryTime

                    print('Fetching asset available')
                    resultAvailable = dbObject.getFreeAsset(gv.dummyIndividualId)
                    print('Asset needed for this trade = ')
                    usedAsset = entryQty*entryPrice
                    print(usedAsset)
                    for freeAssetTotal, dummy1 in resultAvailable:
                        print('Checking if asset is available for this amount')
                        if float(freeAssetTotal)>=usedAsset:
                            print('Inserting Trade -------------------------------------------')
                            dbObject.insertNewTrade(tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice)
                            dbObject.updateIndividualAsset(gv.dummyIndividualId, usedAsset)
                if endTime<dayEndTime:
                    startTime = endTime
                    endTime = endTime + timedelta(hours=gv.hourWindow)
                    print('Not yet done for the day : ' + str(date))
                    print('New start time : ' + str(startTime))
                    print('New end time : ' + str(endTime))
                else:
                    print('Fetching trades that are to exit by the day end')
                    resultTradesExit = dbObject.getTradesExitEnd(date, lastCheckedTime, endTime)
                    for id, qty, price in resultTradesExit:
                        freedAsset = qty*price*(-1)
                        dbObject.updateIndividualAsset(gv.dummyIndividualId, freedAsset)

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
    dbObject.dbClose()
