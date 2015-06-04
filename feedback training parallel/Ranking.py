__author__ = 'Ciddhi'

from PerformanceDrawdown import *
from multiprocessing import Process, Lock, Pool
import logging

def startProcess(work):
    startDate = work[0]
    endDate = work[1]
    individualId = work[2]
    performanceObject = work[3]
    dbObject = DBUtils()
    dbObject.dbConnect()
    lock.acquire()
    logging.info("Starting ranking process for individual " + str(individualId))
    lock.release()
    resultPM = performanceObject.calculatePerformance(startDate, endDate, individualId, dbObject)
    lock.acquire()
    logging.info("Performance for individual id " + str(individualId) + " is " + str(resultPM[0][1]))
    logging.info("Finished ranking process for individual " + str(individualId))
    lock.release()
    return (individualId, resultPM[0][1])

def init(l):
    global lock
    lock = l


class Ranking:

    def updateRankings(self, startDate, endDate, performanceObject, dbObject):
        l = Lock()
        pool = Pool(gv.maxProcesses, initializer=init, initargs=(l,))

        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)
        workList = []

        count = 0
        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            workList.append((startDate, endDate, individualId, performanceObject))
            if count == 10:
                break
            count += 1

        performanceList = pool.map(startProcess, workList)

        pool.close()
        pool.join()
        print(performanceList)

        # Sorting the individuals according to performance
        performanceList.sort(key=lambda tup: -tup[1])

        # Updating ranks in db
        for i in range(0, len(performanceList), 1):
            if performanceList[i][1] != gv.dummyPerformance:
                dbObject.updateRank(performanceList[i][0], i+1)


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