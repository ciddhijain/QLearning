__author__ = 'Ciddhi'

from PerformanceDrawdown import *
from multiprocessing import Lock, Pool
import logging

def startProcess(work):
    startDate = work[0]
    endDate = work[1]
    individualId = work[2]
    performanceObject = work[3]
    dbObject = DBUtils()
    dbObject.dbConnect()
    #print("Starting performance calculation for " + str(individualId))
    resultPM = performanceObject.calculatePerformance(startDate, endDate, individualId, dbObject)
    gv.lock.acquire()
    dbObject.updatePerformance(individualId, resultPM[0][1])
    gv.lock.release()
    dbObject.dbClose()
    #print("Finished performance calculation for " + str(individualId) + ". Performance = " + str(resultPM[0][1]))
    return (individualId, resultPM[0][1])

class Ranking:

    def updateRankings(self, startDate, endDate, performanceObject, dbObject, pool):

        logging.info("Updating ranks for all individuals from " + str(startDate) + " to " + str(endDate))

        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)
        workList = []

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            workList.append((startDate, endDate, individualId, performanceObject))

        pool.map(startProcess, workList)

        # Updating ranks in db
        resultPerformanceList = dbObject.getRankedIndividuals()
        count = 0
        for individualId, dummy in resultPerformanceList:
            dbObject.updateRank(individualId, count+1)
            count += 1

        logging.info("Done Ranking")


if __name__ == "__main__":
    logging.basicConfig(filename='RankingLogs.log', level=logging.INFO, format='%(asctime)s %(message)s')
    dbObject = DBUtils()
    dbObject.dbConnect()
    performanceObj = PerformanceDrawdown()
    #dbObject.initializeRanks()
    rankingObj = Ranking()
    startDate = datetime(2012, 1, 2).date()
    endDate = datetime(2012, 1, 31).date()
    rankingObj.updateRankings(startDate, endDate, performanceObj, dbObject)
    dbObject.dbClose()