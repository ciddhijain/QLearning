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
from multiprocessing import Lock, Pool

def init(l):
    gv.lock = l

if __name__ == "__main__":

    logging.basicConfig(filename=gv.logFileName, level=logging.INFO, format='%(asctime)s %(message)s')

    l = Lock()
    pool = Pool(gv.maxProcesses, initializer=init, initargs=(l,))

    dbObject = DBUtils()
    rankingObject = Ranking()
    mtmObject = MTM()
    rewardMatrixObject = RewardMatrix()
    qMatrixObject = QMatrix()
    trainingObject = Training()
    liveObject = Live()
    reallocationObject = Reallocation()
    plotObject = Plots()
    performanceDrawdownObject = PerformanceDrawdown()
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
    dbObject.dbQuery("DELETE FROM performance_table")
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
    liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
    periodEndDate = gv.endDate
    startTime = timedelta(hours=9, minutes=15)

    dbObject.initializeRanks()
    dbObject.initializePerformance()
    dbObject.resetAssetAllocation(liveStartDate, startTime)
    done = False

    print('Started at : ' + str(datetime.now()))
    while (not done):
        dbObject.resetLatestIndividualsWalkForward()
        dbObject.resetAssetTraining()
        rankingObject.updateRankings(walkforwardStartDate, walkforwardEndDate, performanceDrawdownObject, dbObject, pool)
        trainingObject.train(trainingStartDate, trainingEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, pool)
        liveObject.live(liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject, pool)
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

    pool.close()

    plotObject.plotRefTrades(dbObject)
    plotObject.plotRefPL(dbObject)
    plotObject.plotRefPLPerTrade(dbObject)
    plotObject.plotAsset(liveStartDate, periodEndDate, dbObject)
    plotObject.plotTrades(dbObject)
    plotObject.plotPLPerTrade(dbObject)
    plotObject.plotPL(dbObject)
    [performanceRef, tradesRef] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(liveStartDate, gv.endDate, dbObject)
    [performance, trades] = performanceObject.CalculateReferenceTradesheetPerformanceMeasures(liveStartDate, gv.endDate, dbObject)

    with open(gv.performanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        w.writerow(["original performance", "number of trades"])
        w.writerow([performanceRef, tradesRef])
        w.writerow(["q learning parallel performance", "number of trades"])
        w.writerow([performance, trades])