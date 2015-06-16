__author__ = 'Ciddhi'

class Ranking:

    def updateRankings(self, startDate, endDate, dbObject, performanceDrawdownObject):
        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            resultPM = performanceDrawdownObject.calculatePerformance(startDate, endDate, individualId, dbObject)
            dbObject.updatePerformance(individualId, resultPM[0][1])

        # Updating ranks in db
        resultPerformanceList = dbObject.getRankedIndividuals()
        count = 0
        for individualId, dummy in resultPerformanceList:
            dbObject.updateRank(individualId, count+1)
            count += 1