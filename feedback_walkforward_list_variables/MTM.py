__author__ = 'Ciddhi'

from DBUtils import *
from datetime import timedelta

class MTM:

    def calculateMTM (self, individualId, startDate, startTime, endDate, endTime, dbObject):
        posMtm = 0
        posQty = 0
        negMtm = 0
        negQty = 0
        # Query to get live trades for the individual
        resultTrades = dbObject.getTradesIndividual(individualId, startDate, startTime, endDate, endTime)
        for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
            resultPriceSeries = None
            price = None
            endPrice = None
            resultEndPrice = None
            if entryTime<startTime:
                # To get last price from series to calculate mtm
                resultPrice = dbObject.getPrice(startDate, startTime)
                for time, openPrice in resultPrice:
                    price = openPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            else:
                price = entryPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            # mtm calculation
            if tradeType==1:
                if price and endPrice:
                    mtm = (endPrice-price) * entryQty
                    posMtm += mtm
                    posQty += entryQty
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    negMtm += mtm
                    negQty += entryQty
        return [posMtm, posQty, negMtm, negQty]

    def calculateTrainingMTM (self, individualId, startDate, startTime, endDate, endTime, dbObject):
        posMtm = 0
        posQty = 0
        negMtm = 0
        negQty = 0
        # Query to get live trades for the individual
        resultTrades = dbObject.getTrainingTradesIndividual(individualId, startDate, startTime, endDate, endTime)
        for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
            resultPriceSeries = None
            price = None
            endPrice = None
            resultEndPrice = None
            if entryTime<startTime:
                # To get last price from series to calculate mtm
                resultPrice = dbObject.getPrice(startDate, startTime)
                for time, openPrice in resultPrice:
                    price = openPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            else:
                price = entryPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            # mtm calculation
            if tradeType==1:
                if price and endPrice:
                    mtm = (endPrice-price) * entryQty
                    posMtm += mtm
                    posQty += entryQty
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    negMtm += mtm
                    negQty = entryQty
        return [posMtm, posQty, negMtm, negQty]


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