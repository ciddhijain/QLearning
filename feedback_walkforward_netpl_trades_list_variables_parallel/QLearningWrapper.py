__author__ = 'Ciddhi'

from datetime import timedelta, datetime
from Reallocation import *
from RewardMatrix import *
from Training import *
from Live import *
from NetPL_Trades import *
from Ranking import *
from QMatrix import *
from PerformanceMeasures import *
from PerformanceDrawdown import *
from Setup import *
import calendar

class QLearningWrapper:

    def feedback(self, alpha, gamma, beta, individualFactor, zeroRange, greedyLevel, workId):

        logging.basicConfig(filename=gv.logFileName + str(workId) + '.log', level=logging.INFO, format='%(asctime)s %(message)s')

        print(str(datetime.now()) + " Starting Q Learning for : ")
        print("alpha = " + str(alpha))
        print("gamma = " + str(gamma))
        print("beta = " + str(beta))
        print("individual factor = " + str(individualFactor))
        print("zero range = " + str(zeroRange))
        print("greedy level = " + str(greedyLevel))
        logging.info("Starting Q Learning for : ")
        logging.info("alpha = " + str(alpha))
        logging.info("gamma = " + str(gamma))
        logging.info("beta = " + str(beta))
        logging.info("individual factor = " + str(individualFactor))
        logging.info("zero range = " + str(zeroRange))
        logging.info("greedy level = " + str(greedyLevel))
        logging.info("\n")

        setupObject = Setup(alpha, gamma, beta, individualFactor, zeroRange, greedyLevel)
        [variableString, latestIndividualTable, trainingTradesheetTable, trainingAssetTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable] = setupObject.createQLearningTables()

        dbObject = DBUtils(alpha, gamma, individualFactor, zeroRange, greedyLevel, latestIndividualTable, trainingTradesheetTable, trainingAssetTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable)
        mtmObject = NetPL_Trades()
        rewardMatrixObject = RewardMatrix(alpha)
        qMatrixObject = QMatrix(gamma, greedyLevel, beta)
        trainingObject = Training()
        liveObject = Live()
        reallocationObject = Reallocation()
        performanceObject = PerformanceMeasures()
        performanceOutfileName = gv.performanceOutfileNameBase + variableString + '.csv'
        performanceMonthlyOutfileName = gv.performanceMonthlyOutfileNameBase + variableString + '.csv'

        dbObject.dbConnect()

        dbObject.dbQuery("DELETE FROM " + assetTable)
        dbObject.dbQuery("DELETE FROM " + dailyAssetTable)
        dbObject.dbQuery("DELETE FROM " + newTradesheetTable)
        dbObject.dbQuery("DELETE FROM " + reallocationTable)
        dbObject.dbQuery("DELETE FROM " + qMatrixTable)
        dbObject.dbQuery("DELETE FROM " + trainingAssetTable)
        dbObject.dbQuery("DELETE FROM " + trainingTradesheetTable)
        dbObject.dbQuery("DELETE FROM " + latestIndividualTable)

        rankingStartDate = gv.startDate
        rankingEndDate = rankingStartDate + timedelta(days=gv.rankingDays)
        trainingStartDate = rankingEndDate + timedelta(days=1)
        trainingEndDate = trainingStartDate + timedelta(days=gv.initializationDays)
        liveStartDate = trainingEndDate + timedelta(days=1)
        testingStartDate = liveStartDate
        liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
        testingEndDate = liveEndDate
        periodEndDate = gv.endDate
        startTime = timedelta(hours=9, minutes=15)
        walkforward = 1

        dbObject.resetAssetAllocation(liveStartDate, startTime)

        done = False

        while (not done):
            dbObject.resetAssetTraining()
            trainingObject.train(trainingStartDate, trainingEndDate, walkforward, dbObject, mtmObject, rewardMatrixObject, qMatrixObject)
            dbObject.resetLatestIndividualsWalkForward()
            liveObject.live(liveStartDate, liveEndDate, walkforward, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
            walkforward += 1
            if liveEndDate>=periodEndDate:
                done = True
            else:
                dbObject.updateQMatrixTableWalkForward()
                dbObject.updateAssetWalkForward()
                trainingEndDate = liveEndDate
                trainingStartDate = trainingEndDate - timedelta(days=gv.initializationDays)
                liveStartDate = liveEndDate + timedelta(days=1)
                liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
                if liveEndDate>periodEndDate:
                    liveEndDate = periodEndDate

        with open(performanceOutfileName, 'w') as fp:
            w = csv.writer(fp)
            w.writerow(["original performance", "number of trades", "q learning performance", "number of trades"])
            [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(testingStartDate, periodEndDate, dbObject)
            [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(testingStartDate, periodEndDate, dbObject)
            [performanceTraining, tradesTraining] = performanceObject.CalculateTrainingTradesheetPerformanceMeasures(testingStartDate, periodEndDate, dbObject)
            w.writerow([performanceRef, tradesRef, performance, trades, performanceTraining, tradesTraining])

        done = False
        with open(performanceMonthlyOutfileName, 'w') as fp:
            w = csv.writer(fp)
            w.writerow(["original performance", "number of trades", "q learning performance", "number of trades", "q learning training performance", "number of trades"])
            #w.writerow(["q learning performance", "number of trades"])
            while not done:
                [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
                [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
                [performanceTraining, tradesTraining] = performanceObject.CalculateTrainingTradesheetPerformanceMeasures(testingStartDate, testingEndDate, dbObject)
                w.writerow([performanceRef, tradesRef, performance, trades, performanceTraining, tradesTraining])
                #w.writerow([performance, trades])
                if testingEndDate>=periodEndDate:
                    done = True
                else:
                    testingStartDate = testingEndDate + timedelta(days=1)
                    testingEndDate = testingStartDate + timedelta(days=gv.liveDays)
                    if testingEndDate>periodEndDate:
                        testingEndDate = periodEndDate
        dbObject.dbClose()