__author__ = 'Ciddhi'

from DBUtils import *
from datetime import datetime

class PerformanceMeasures:

    def CalculateTradesheetPerformanceMeasures(self, startDate, endDate, dbObject):
        resultLongNetPL = dbObject.getLongNetPL(startDate, endDate)
        resultShortNetPL = dbObject.getShortNetPL(startDate, endDate)
        resultLongTrades = dbObject.getLongTrades(startDate, endDate)
        resultShortTrades = dbObject.getShortTrades(startDate, endDate)
        netPL = 0
        totalTrades = 0
        for pl, dummy in resultLongNetPL:
            netPL = netPL + pl
        for pl, dummy in resultShortNetPL:
            netPL = netPL + pl
        for trades, dummy in resultLongTrades:
            totalTrades = totalTrades + trades
        for trades, dummy in resultShortTrades:
            totalTrades = totalTrades + trades
        performace = netPL/totalTrades
        print(totalTrades)
        print(performace)
        return performace

    def CalculateReferenceTradesheetPerformanceMeasures(self, startDate, endDate, dbObject):
        resultLongNetPL = dbObject.getRefLongNetPL(startDate, endDate)
        resultShortNetPL = dbObject.getRefShortNetPL(startDate, endDate)
        resultLongTrades = dbObject.getRefLongTrades(startDate, endDate)
        resultShortTrades = dbObject.getRefShortTrades(startDate, endDate)
        netPL = 0
        totalTrades = 0
        for pl, dummy in resultLongNetPL:
            netPL = netPL + pl
        for pl, dummy in resultShortNetPL:
            netPL = netPL + pl
        for trades, dummy in resultLongTrades:
            totalTrades = totalTrades + trades
        for trades, dummy in resultShortTrades:
            totalTrades = totalTrades + trades
        performace = netPL/totalTrades
        print(totalTrades)
        print(performace)
        return performace


if __name__ == "__main__":
    dbObject = DBUtils()
    performanceObject = PerformanceMeasures()
    date = datetime(2012, 4, 9).date()
    periodEndDate = datetime(2013, 9, 3).date()
    dbObject.dbConnect()
    performanceObject.CalculateTradesheetPerformanceMeasures(date, periodEndDate, dbObject)
    performanceObject.CalculateReferenceTradesheetPerformanceMeasures(date, periodEndDate, dbObject)
    dbObject.dbClose()

