__author__ = 'Ciddhi'

from DBUtils import *
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

class Plots:
    def plotAsset(self, startDate, endDate, dbObject):
        startDateMonth = int(startDate.month)
        endDateMonth = int(endDate.month)
        startDateYear = int(startDate.year)
        endDateYear = int(endDate.year)
        dates = []
        dummyDates = []
        assetValues = []
        minVales = []
        maxValues = []
        count = 0
        done = False
        while (not done):
            resultMonthEndAsset = dbObject.getAssetMonthly(startDateMonth, startDateYear)
            resultMaxMinAsset = dbObject.getAssetMonthlyMaxMin(startDateMonth, startDateYear)
            for asset, dummy in resultMonthEndAsset:
                count += 1
                dates.append(str(startDateMonth) + "/" + str(startDateYear))
                dummyDates.append(count)
                assetValues.append(asset)
            for max, min in resultMaxMinAsset:
                if max is not None:
                    maxValues.append(max)
                    minVales.append(min)
            if startDateYear<endDateYear:
                startDateYear = startDateYear + startDateMonth/12
                startDateMonth = startDateMonth % 12 + 1
            elif startDateMonth<endDateMonth:
                startDateMonth = startDateMonth % 12 + 1
            else:
                done = True
        plt.figure(1)
        plt.bar(dummyDates, assetValues, align='center', width=0.1)
        plt.plot(dummyDates, maxValues, '--g^', label='Max Asset in Month')
        plt.plot(dummyDates, minVales, '--ro', label='Min Asset in Month')
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('Asset')
        plt.title('Asset Variation in Testing Period')
        plt.legend(loc="center right", bbox_to_anchor=(1.3,0.5))
        plt.savefig('asset.png')

    def plotTrades(self, dbObject):
        dates = []
        dummyDates = []
        numTrades = []
        count = 0
        resultTrades = dbObject.getTradesMonthly()
        for trades, month, year in resultTrades:
            count += 1
            dummyDates.append(count)
            dates.append(str(month) + "/" + str(year))
            numTrades.append(trades)
        plt.figure(2)
        plt.bar(dummyDates, numTrades, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('Number of trades')
        plt.title('Trades per Month')
        plt.savefig('trades_per_month.png')

    def plotPLPerTrade(self, dbObject):
        dates = []
        dummyDates = []
        netPL = []
        months = []
        years = []
        trades = []
        count = 0
        resultLongPL = dbObject.getNetPLLongMonthly()
        resultShortPL = dbObject.getNetPLShortMonthly()
        for longPL, trade, month, year in resultLongPL:
            count += 1
            dummyDates.append(count)
            months.append(month)
            years.append(year)
            dates.append(str(month) + "/" + str(year))
            netPL.append(longPL)
            trades.append(trade)
        count=0
        for shortPL, trade, month, year in resultShortPL:
            netPL[count] = netPL[count] + shortPL
            trades[count] = trades[count] + trade
            netPL[count] = netPL[count]/trades[count]
            count = count + 1
        plt.figure(3)
        plt.bar(dummyDates, netPL, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('NetPL per trade')
        plt.title('NetPL per trade per month')
        plt.savefig('netpl_per_trade_per_month.png')

    def plotPL(self, dbObject):
        dates = []
        dummyDates = []
        netPL = []
        months = []
        years = []
        count = 0
        resultLongPL = dbObject.getNetPLLongMonthly()
        resultShortPL = dbObject.getNetPLShortMonthly()
        for longPL, trades, month, year in resultLongPL:
            count += 1
            dummyDates.append(count)
            months.append(month)
            years.append(year)
            dates.append(str(month) + "/" + str(year))
            netPL.append(longPL)
        count=0
        for shortPL, trades, month, year in resultShortPL:
            netPL[count] = netPL[count] + shortPL
            count = count + 1
        plt.figure(4)
        plt.bar(dummyDates, netPL, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('NetPL')
        plt.title('NetPL per month')
        plt.savefig('netpl_per_month.png')

    def plotRefTrades(self, dbObject):
        dates = []
        dummyDates = []
        numTrades = []
        count = 0
        resultTrades = dbObject.getRefTradesMonthly()
        for trades, month, year in resultTrades:
            count += 1
            dummyDates.append(count)
            dates.append(str(month) + "/" + str(year))
            numTrades.append(trades)
        plt.figure(5)
        plt.bar(dummyDates, numTrades, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('Number of trades')
        plt.title('Trades per Month in base tradesheet')
        plt.savefig('trades_per_month_base.png')

    def plotRefPLPerTrade(self, dbObject):
        dates = []
        dummyDates = []
        netPL = []
        months = []
        years = []
        trades = []
        count = 0
        resultLongPL = dbObject.getRefNetPLLongMonthly()
        resultShortPL = dbObject.getRefNetPLShortMonthly()
        for longPL, trade, month, year in resultLongPL:
            count += 1
            dummyDates.append(count)
            months.append(month)
            years.append(year)
            dates.append(str(month) + "/" + str(year))
            netPL.append(longPL)
            trades.append(trade)
        count=0
        for shortPL, trade, month, year in resultShortPL:
            netPL[count] = netPL[count] + shortPL
            trades[count] = trades[count] + trade
            netPL[count] = netPL[count]/trades[count]
            count = count + 1
        #del netPL[-1]
        #del dummyDates[-1]
        #del dates[-1]
        plt.figure(6)
        plt.bar(dummyDates, netPL, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('NetPL per trade')
        plt.title('NetPL per trade per month in base tradesheet')
        plt.savefig('netpl_per_trade_per_month_base.png')

    def plotRefPL(self, dbObject):
        dates = []
        dummyDates = []
        netPL = []
        months = []
        years = []
        count = 0
        resultLongPL = dbObject.getRefNetPLLongMonthly()
        resultShortPL = dbObject.getRefNetPLShortMonthly()
        for longPL, trades, month, year in resultLongPL:
            count += 1
            dummyDates.append(count)
            months.append(month)
            years.append(year)
            dates.append(str(month) + "/" + str(year))
            netPL.append(longPL)
        count=0
        for shortPL, trades, month, year in resultShortPL:
            netPL[count] = netPL[count] + shortPL
            count = count + 1
        plt.figure(7)
        plt.bar(dummyDates, netPL, align='center', width=0.1)
        plt.xticks(dummyDates, dates, rotation='vertical')
        plt.xlabel('Month of the Year')
        plt.ylabel('NetPL')
        plt.title('NetPL per month in base tradesheet')
        plt.savefig('netpl_per_month_base')


if __name__ == "__main__":
    dbObject = DBUtils()
    plotObject = Plots()
    date = datetime(2012, 1, 2).date()
    periodEndDate = datetime(2012, 12, 31).date()
    dbObject.dbConnect()
    plotObject.plotAsset(date, periodEndDate, dbObject)
    #plotObject.plotTrades(dbObject)
    #plotObject.plotPLPerTrade(dbObject)
    #plotObject.plotPL(dbObject)
    #plotObject.plotRefTrades(dbObject)
    #plotObject.plotRefPL(dbObject)
    #plotObject.plotRefPLPerTrade(dbObject)
    dbObject.dbClose()


