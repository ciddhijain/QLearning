__author__ = 'Ciddhi'

from Drawdown import *

class Ranking:

    def updateRankings(self, startDate, endDate, dbObject):
        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)
        ddObject = Drawdown()

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            resultPM = ddObject.calculatePerformance(startDate, endDate, individualId, dbObject)
            dbObject.updatePerformance(individualId, resultPM[0][1])

        # Updating ranks in db
        resultPerformanceList = dbObject.getRankedIndividuals()
        count = 0
        for individualId, dummy in resultPerformanceList:
            dbObject.updateRank(individualId, count+1)
            count += 1

if __name__ == "__main__":
    rankingObject = Ranking()
    dbObject = DBUtils()
    dbObject.dbConnect()
    date = datetime(2012, 1, 2).date()
    periodEndDate = datetime(2012, 1, 10).date()
    rankingObject.updateRankings(date, periodEndDate, dbObject)
    dbObject.dbClose()