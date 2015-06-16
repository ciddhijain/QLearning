__author__ = 'Ciddhi'

from datetime import timedelta

class MTMOfflineCalculation:

    def calculateDailyMTM(self, startDate, endDate, dbObject):
        date = startDate
        periodEndDate = endDate
        done = False

        while not done:
            resultTrades = dbObject.getRefDayTrades(date)
            for tradeId, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
                mtm = 0
                if tradeType==1:             # For long trade
                    mtm = (exitPrice-entryPrice) * entryQty
                else:
                    mtm = (entryPrice-exitPrice) * entryQty
                dbObject.insertOrUpdateMTM(individualId, date, mtm)
            date = date + timedelta(days=1)
            if(date>periodEndDate):
                done = True