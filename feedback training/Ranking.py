__author__ = 'Ciddhi'

from Drawdown import *

class Ranking:

    def updateRankings(self, startDate, endDate, dbObject):
        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)
        individualList = []
        performanceList = []
        ddObject = Drawdown()

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            resultPM = ddObject.calculatePerformance(startDate, endDate, individualId, dbObject)
            individualList.append(individualId)
            performanceList.append(resultPM[0][1])

        # Sorting the individuals according to performance
        individualPerformance = list(zip(individualList, performanceList))
        individualPerformance.sort(key=lambda tup: -tup[1])

        # Updating ranks in db
        for i in range(0, len(individualList), 1):
            if individualPerformance[i][1] != gv.dummyPerformance:
                dbObject.updateRank(individualPerformance[i][0], i+1)

if __name__ == "__main__":
    rankingObject = Ranking()
    dbObject = DBUtils()
    dbObject.dbConnect()
    date = datetime(2012, 1, 2).date()
    periodEndDate = datetime(2012, 1, 10).date()
    rankingObject.updateRankings(date, periodEndDate, dbObject)
    dbObject.dbClose()