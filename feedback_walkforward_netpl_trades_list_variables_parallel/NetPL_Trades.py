__author__ = 'Ciddhi'

from DBUtils import *
from datetime import timedelta

class NetPL_Trades:

    def calculateNetPLTrades (self, individualId, startDate, startTime, endDate, endTime, dbObject):
        posMtm = 0
        posTrades = 0
        negMtm = 0
        negTrades = 0
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
                    posTrades += 1
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    negMtm += mtm
                    negTrades += 1
        return [posMtm, posTrades, negMtm, negTrades]

    def calculateTrainingNetPLTrades (self, individualId, startDate, startTime, endDate, endTime, dbObject):
        posMtm = 0
        posTrades = 0
        negMtm = 0
        negTrades = 0
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
                    posTrades += 1
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    negMtm += mtm
                    negTrades += 1
        return [posMtm, posTrades, negMtm, negTrades]