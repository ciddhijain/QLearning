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
import calendar

if __name__ == "__main__":

    dbObject = DBUtils()
    rankingObject = Ranking()
    mtmObject = MTM()
    rewardMatrixObject = RewardMatrix()
    qMatrixObject = QMatrix()
    trainingObject = Training()
    liveObject = Live()
    reallocationObject = Reallocation()
    plotObject = Plots()
    performanceObject = PerformanceMeasures()

    dbObject.dbConnect()

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

    walkforwardStartDate = gv.startDate
    walkforwardEndDate = datetime(walkforwardStartDate.year, walkforwardStartDate.month, calendar.monthrange(walkforwardStartDate.year, walkforwardStartDate.month)[1]).date()
    trainingStartDate = walkforwardEndDate + timedelta(days=1)
    trainingEndDate = datetime(trainingStartDate.year, trainingStartDate.month, calendar.monthrange(trainingStartDate.year, trainingStartDate.month)[1]).date()
    liveStartDate = trainingEndDate + timedelta(days=1)
    liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
    periodEndDate = gv.endDate
    startTime = timedelta(hours=9, minutes=15)

    dbObject.initializeRanks()
    dbObject.resetAssetAllocation(liveStartDate, startTime)
    done = False

    print('Started at : ' + str(datetime.now()))
    while (not done):
        dbObject.resetLatestIndividualsWalkForward()
        dbObject.resetAssetTraining()
        rankingObject.updateRankings(walkforwardStartDate, walkforwardEndDate, dbObject)
        trainingObject.train(trainingStartDate, trainingEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject)
        liveObject.live(liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
        if liveEndDate>=periodEndDate:
            done = True
        else:
            dbObject.updateQMatrixTableWalkForward()
            dbObject.updateAssetWalkForward()
            dbObject.resetRanks()
            walkforwardStartDate = trainingStartDate
            walkforwardEndDate = trainingEndDate
            trainingStartDate = liveStartDate
            trainingEndDate = liveEndDate
            liveStartDate = trainingEndDate + timedelta(days=1)
            liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
            if liveEndDate>periodEndDate:
                liveEndDate = periodEndDate
    print('Finished at : ' + str(datetime.now()))


    plotObject.plotAsset(gv.startDate, gv.endDate, dbObject)
    plotObject.plotTrades(dbObject)
    plotObject.plotPLPerTrade(dbObject)
    plotObject.plotPL(dbObject)
    plotObject.plotRefTrades(dbObject)
    plotObject.plotRefPL(dbObject)
    plotObject.plotRefPLPerTrade(dbObject)

    [performance, trades] = performanceObject.CalculateTradesheetPerformanceMeasures(gv.startDate, gv.endDate, dbObject)
    [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(gv.startDate, gv.endDate, dbObject)

    with open(gv.performanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        w.writerow(["original performance", "number of trades"])
        w.writerow([performanceRef, tradesRef])
        w.writerow(["q learning performance", "number of trades"])
        w.writerow([performance, trades])