__author__ = 'Ciddhi'

from datetime import timedelta, datetime
from Reallocation import *
from RewardMatrix import *
from Training import *
from Live import *
from MTM import *
from Ranking import *
from QMatrix import *
from PerformanceMeasures import *
from PerformanceDrawdown import *
from Plots import *
from Setup import *
import calendar

class QLearningWrapper:

    def feedback(self, alpha, gamma, individualFactor, zeroRange, greedyLevel):

        setupObject = Setup(alpha, gamma, individualFactor, zeroRange, greedyLevel)
        [variableString, latestIndividualTable, trainingTradesheetTable, trainingAssetTable, rankingTable, performanceTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable] = setupObject.createQLearningTables()

        dbObject = DBUtils(alpha, gamma, individualFactor, zeroRange, greedyLevel, latestIndividualTable, trainingTradesheetTable, trainingAssetTable, rankingTable, performanceTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable)
        rankingObject = Ranking()
        mtmObject = MTM()
        rewardMatrixObject = RewardMatrix(alpha)
        qMatrixObject = QMatrix(gamma, greedyLevel)
        trainingObject = Training()
        liveObject = Live()
        reallocationObject = Reallocation()
        plotObject = Plots()
        performanceObject = PerformanceMeasures()
        performanceDrawdownObject = PerformanceDrawdown()
        performanceOutfileName = gv.performanceOutfileNameBase + variableString
        performanceMonthlyOutfileName = gv.performanceMonthlyOutfileNameBase + variableString

        dbObject.dbConnect()

        dbObject.dbQuery("DELETE FROM " + assetTable)
        dbObject.dbQuery("DELETE FROM " + dailyAssetTable)
        dbObject.dbQuery("DELETE FROM " + newTradesheetTable)
        dbObject.dbQuery("DELETE FROM " + reallocationTable)
        dbObject.dbQuery("DELETE FROM " + qMatrixTable)
        dbObject.dbQuery("DELETE FROM " + trainingAssetTable)
        dbObject.dbQuery("DELETE FROM " + trainingTradesheetTable)
        dbObject.dbQuery("DELETE FROM " + rankingTable)
        dbObject.dbQuery("DELETE FROM " + latestIndividualTable)
        dbObject.dbQuery("DELETE FROM " + performanceTable)

        walkforwardStartDate = gv.startDate
        walkforwardEndDate = walkforwardStartDate + timedelta(days=1)
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = trainingStartDate + timedelta(days=1)
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = liveStartDate + timedelta(days=1)
        periodEndDate = walkforwardStartDate + timedelta(days=7)
        testingStartDate = liveStartDate
        testingEndDate = liveEndDate
        startTime = timedelta(hours=9, minutes=15)
        '''

        walkforwardStartDate = gv.startDate
        walkforwardEndDate = datetime(walkforwardStartDate.year, walkforwardStartDate.month, calendar.monthrange(walkforwardStartDate.year, walkforwardStartDate.month)[1]).date()
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = datetime(trainingStartDate.year, trainingStartDate.month, calendar.monthrange(trainingStartDate.year, trainingStartDate.month)[1]).date()
        liveStartDate = trainingEndDate + timedelta(days=1)
        testingStartDate = liveStartDate
        liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
        testingEndDate = liveEndDate
        periodEndDate = gv.endDate
        startTime = timedelta(hours=9, minutes=15)
        '''

        dbObject.initializeRanks()
        dbObject.initializePerformance()
        dbObject.resetAssetAllocation(liveStartDate, startTime)
        done = False

        while (not done):
            dbObject.resetAssetTraining()
            rankingObject.updateRankings(walkforwardStartDate, walkforwardEndDate, dbObject, performanceDrawdownObject)
            trainingObject.train(trainingStartDate, trainingEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject)
            dbObject.resetLatestIndividualsWalkForward()
            liveObject.live(liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
            if liveEndDate>=periodEndDate:
                done = True
            else:
                dbObject.updateQMatrixTableWalkForward()
                dbObject.updateAssetWalkForward()
                dbObject.resetRanks()
                dbObject.resetPerformance()
                walkforwardStartDate = trainingStartDate
                walkforwardEndDate = trainingEndDate
                trainingStartDate = liveStartDate
                trainingEndDate = liveEndDate
                liveStartDate = trainingEndDate + timedelta(days=1)
                liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
                if liveEndDate>periodEndDate:
                    liveEndDate = periodEndDate

        plotObject.plotRefTrades(dbObject)
        plotObject.plotRefPL(dbObject)
        plotObject.plotRefPLPerTrade(dbObject)
        plotObject.plotAsset(testingStartDate, gv.endDate, dbObject)
        plotObject.plotTrades(dbObject)
        plotObject.plotPL(dbObject)
        plotObject.plotPLPerTrade(dbObject)

        with open(performanceOutfileName, 'w') as fp:
            w = csv.writer(fp)
            w.writerow(["original performance", "number of trades", "q learning performance", "number of trades"])
            [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(testingStartDate, periodEndDate, dbObject)
            [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(testingStartDate, periodEndDate, dbObject)
            w.writerow([performanceRef, tradesRef, performance, trades])

        done = False
        with open(performanceMonthlyOutfileName, 'w') as fp:
            w = csv.writer(fp)
            w.writerow(["original performance", "number of trades", "q learning performance", "number of trades"])
            #w.writerow(["q learning performance", "number of trades"])
            while not done:
                [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
                [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
                w.writerow([performanceRef, tradesRef, performance, trades])
                #w.writerow([performance, trades])
                if testingEndDate>=periodEndDate:
                    done = True
                else:
                    testingStartDate = testingEndDate + timedelta(days=1)
                    testingEndDate = datetime(testingStartDate.year, testingStartDate.month, calendar.monthrange(testingStartDate.year, testingStartDate.month)[1]).date()
                    if testingEndDate>periodEndDate:
                        testingEndDate = periodEndDate
        dbObject.dbClose()