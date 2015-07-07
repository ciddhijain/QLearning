__author__ = 'Ciddhi'

import GlobalVariables as gv
from QLearningWrapper import *
import logging
from MTMOfflineCalculation import *
from datetime import datetime
from multiprocessing import Pool

def startProcess(work):
    alpha = work[0]
    gamma = work[1]
    beta = work[2]
    individualFactor = work[3]
    zeroRange = work[4]
    greedyLevel = work[5]
    workId = work[6]
    qLearningObject = QLearningWrapper()
    qLearningObject.feedback(alpha, gamma, beta, individualFactor, zeroRange, greedyLevel, workId)
    return 0

if __name__ == "__main__":

    logging.basicConfig(filename=gv.logFileName + 'Main.log', level=logging.INFO, format='%(asctime)s %(message)s')

    pool = Pool(gv.maxProcesses)

    alphaList = gv.alpha
    gammaList = gv.gamma
    betaList = gv.beta
    individualFactorList = gv.individualFactor
    zeroRangeList = gv.zeroRange
    greedyLevelList = gv.maxGreedyLevel


    performanceDrawdownObject = PerformanceDrawdown()
    rankingObject = Ranking()
    mtmOfflineObject = MTMOfflineCalculation()
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("DELETE FROM " + gv.rankingTableBase)
    dbObject.dbQuery("DELETE FROM " + gv.performanceTableBase)
    dbObject.dbQuery("DELETE FROM " + gv.rankingWalkforwardTableBase)
    dbObject.dbQuery("DELETE FROM " + gv.dailyMtmTableBase)

    mtmStartDate = gv.startDate
    mtmEndDate = gv.endDate - timedelta(days=gv.liveDays) - timedelta(days=gv.initializationDays) + timedelta(days=1)
    logging.info("Starting mtm calculation")
    mtmOfflineObject.calculateDailyMTM(mtmStartDate, mtmEndDate, dbObject)

    rankingStartDate = gv.startDate
    rankingEndDate = rankingStartDate + timedelta(days=gv.rankingDays)
    trainingStartDate = rankingEndDate + timedelta(days=1)
    trainingEndDate = trainingStartDate + timedelta(days=gv.initializationDays)
    liveStartDate = trainingEndDate + timedelta(days=1)
    liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
    periodEndDate = gv.endDate
    done = False
    rankingWalkforward = 1

    while not done:
        logging.info("Updating ranks for walkforward - " + str(rankingWalkforward))
        rankingObject.updateRankings(rankingStartDate, rankingEndDate, rankingWalkforward, dbObject, performanceDrawdownObject)
        rankingWalkforward += 1
        if liveEndDate>=periodEndDate:
            done = True
        else:
            trainingEndDate = liveEndDate
            trainingStartDate = trainingEndDate - timedelta(days=gv.initializationDays)
            rankingEndDate = trainingStartDate - timedelta(days=1)
            rankingStartDate = rankingEndDate - timedelta(days=gv.rankingDays)
            liveStartDate = liveEndDate + timedelta(days=1)
            liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
            if liveEndDate>periodEndDate:
                liveEndDate = periodEndDate

    dbObject.dbClose()

    workList = []
    workId = 0

    for individualFactor in individualFactorList:
        for greedyLevel in greedyLevelList:
            for zeroRange in zeroRangeList:
                for alpha in alphaList:
                    for gamma in gammaList:
                        for beta in betaList:
                            workList.append((alpha, gamma, beta, individualFactor, zeroRange, greedyLevel, workId))
                            workId += 1

    logging.info("Done Ranking. Calling processes now.")
    pool.map(startProcess, workList)