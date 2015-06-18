__author__ = 'Ciddhi'

from DBUtils import *
from datetime import timedelta

class MTM:

    # Optimize so that at every call, only the new values are calculated. Add concept of start time for mtm calculation
    def calculateMTM (self, aggregationUnit, startDate, startTime, endDate, endTime, dbObject):
        # Query to get live trades for the individual

        resultTrades = dbObject.getTrades(startDate, startTime, endDate, endTime)

        for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime in resultTrades:
            resultPriceSeries = None
            price = None
            endPrice = None
            resultEndPrice = None

            if entryTime<startTime:

                # To get last price from series to calculate mtm
                resultPrice = dbObject.getPrice(startDate, startTime)
                for time, openPrice in resultPrice:
                    price = openPrice

                # Use when mtm is to be calculated at every aggregation unit
                '''
                if exitTime>=endTime:
                    resultPriceSeries = dbObject.getPriceSeries(startDate, startTime, endDate, endTime)
                else:
                    resultPriceSeries = dbObject.getPriceSeries(startDate, startTime, endDate, exitTime)
                '''

                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p

            else:

                price = entryPrice

                # Use when mtm is to be calculated at every aggregation unit
                '''
                if exitTime>=endTime:
                    resultPriceSeries = dbObject.getPriceSeries(startDate, entryTime, endDate, endTime)
                else:
                    resultPriceSeries = dbObject.getPriceSeries(startDate, entryTime, endDate, exitTime)
                '''

                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p

            # mtm calculation
            # Use when mtm is to be calculated at every aggregation unit
            '''
            count = 0
            for mtmTime, openPrice in resultPriceSeries:
                if (count%aggregationUnit==0):
                    mtm = (openPrice-price) * entryQty
                    #print(mtm)
                    dbObject.insertMTM(individualId, tradeId, tradeType, entryDate, mtmTime, mtm)
                    price = openPrice
                count = count+1
            '''
            if tradeType==0:
                mtm = (endPrice-price) * entryQty
                dbObject.insertMTM(individualId, tradeId, tradeType, entryDate, endTime, mtm)
            else:
                mtm = (price-endPrice) * entryQty
                dbObject.insertMTM(individualId, tradeId, tradeType, entryDate, endTime, mtm)

if __name__ == "__main__":
    mtmObject = MTM()
    aggregationUnit = 1
    startDate = 20120409
    startTime = timedelta(hours=10, minutes=30)
    endDate = 20120409
    endTime = timedelta(hours=11, minutes=30)
    dbObject = DBUtils()
    dbObject.dbConnect()

    # Reset mtm_table
    #queryReset = "DELETE FROM mtm_table"
    #dbObject.dbQuery(queryReset)
    #mtmObject.calculateMTM(2, aggregationUnit)
    mtmObject.calculateMTM(aggregationUnit, startDate, startTime, endDate, endTime, dbObject)

    #dbObject.insertMTM(1, 11296331, 0, startDate, startTime, 0)
    dbObject.dbClose()