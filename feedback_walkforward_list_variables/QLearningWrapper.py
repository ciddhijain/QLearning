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
from Plots import *
from Setup import *
import calendar

class QLearningWrapper:
    def feedback(self, alpha, gamma, individualFactor, zeroRange, greedyLevel):

        setupObject = Setup(alpha, gamma, individualFactor, zeroRange, greedyLevel)
        dbObject = DBUtils(alpha, gamma, individualFactor, zeroRange, greedyLevel)
        rankingObject = Ranking()
        mtmObject = MTM()
        rewardMatrixObject = RewardMatrix(alpha)
        qMatrixObject = QMatrix(gamma, greedyLevel)
        trainingObject = Training()
        liveObject = Live()
        reallocationObject = Reallocation()
        plotObject = Plots()
        performanceObject = PerformanceMeasures()
        performanceOutfileName = gv.performanceOutfileNameBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        performanceMonthlyOutfileName = gv.performanceMonthlyOutfileNameBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)

        dbObject.dbConnect()
        setupObject.createTables(dbObject)

        dbObject.dbQuery("DELETE FROM asset_allocation_table")
        dbObject.dbQuery("DELETE FROM asset_daily_allocation_table")
        dbObject.dbQuery("DELETE FROM mtm_table")
        dbObject.dbQuery("DELETE FROM tradesheet_data_table")
        dbObject.dbQuery("DELETE FROM reallocation_table")
        dbObject.dbQuery("DELETE FROM q_matrix_table")
        dbObject.dbQuery("DELETE FROM training_asset_allocation_table")
        dbObject.dbQuery("DELETE FROM training_mtm_table")
        dbObject.dbQuery("DELETE FROM training_tradesheet_data_table")
        dbObject.dbQuery("DELETE FROM ranking_table")
        dbObject.dbQuery("DELETE FROM latest_individual_table")
        '''

        walkforwardStartDate = gv.startDate
        walkforwardEndDate = walkforwardStartDate + timedelta(days=1)
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = trainingStartDate + timedelta(days=1)
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = liveStartDate + timedelta(days=1)
        periodEndDate = walkforwardStartDate + timedelta(days=12)
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

        dbObject.initializeRanks()
        dbObject.initializePerformance()
        dbObject.resetAssetAllocation(liveStartDate, startTime)
        done = False

        print('Started at : ' + str(datetime.now()))
        while (not done):
            dbObject.resetAssetTraining()
            rankingObject.updateRankings(walkforwardStartDate, walkforwardEndDate, dbObject)
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
        print('Finished at : ' + str(datetime.now()))

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
            [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
            [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
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