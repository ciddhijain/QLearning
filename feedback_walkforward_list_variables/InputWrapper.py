__author__ = 'Ciddhi'

import GlobalVariables as gv
from QLearningWrapper import *
import logging
from datetime import datetime

if __name__ == "__main__":
    alphaList = gv.alpha
    gammaList = gv.gamma
    individualFactorList = gv.individualFactor
    zeroRangeList = gv.zeroRange
    greedyLevelList = gv.maxGreedyLevel

    qLearningObject = QLearningWrapper()
    performanceDrawdownObject = PerformanceDrawdown()
    rankingObject = Ranking()
    dbObject = DBUtils()
    dbObject.dbConnect()

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

    logging.basicConfig(filename=gv.logFileName, level=logging.INFO, format='%(asctime)s %(message)s')

    for alpha in alphaList:
        for gamma in gammaList:
            for individualFactor in individualFactorList:
                for zeroRange in zeroRangeList:
                    for greedyLevel in greedyLevelList:
                        print(str(datetime.now()) + "Starting Q Learning for : ")
                        print("alpha = " + str(alpha))
                        print("gamma = " + str(gamma))
                        print("individual factor = " + str(individualFactor))
                        print("zero range = " + str(zeroRange))
                        print("greedy level = " + str(greedyLevel))
                        logging.info("Starting Q Learning for : ")
                        logging.info("alpha = " + str(alpha))
                        logging.info("gamma = " + str(gamma))
                        logging.info("individual factor = " + str(individualFactor))
                        logging.info("zero range = " + str(zeroRange))
                        logging.info("greedy level = " + str(greedyLevel))
                        logging.info("\n")
                        qLearningObject.feedback(alpha, gamma, individualFactor, zeroRange, greedyLevel)
                        print("\n")
