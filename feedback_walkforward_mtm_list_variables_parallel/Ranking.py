__author__ = 'Ciddhi'

import logging

class Ranking:

    def updateRankings(self, startDate, endDate, rankingWalkforward, dbObject, performanceDrawdownObject):

        resultIndividuals = dbObject.getAllRefIndividuals()
        dbObject.insertRankingWalkforward(startDate, endDate, rankingWalkforward)

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            performance = performanceDrawdownObject.calculateIndividualPerformanceFromMTM(startDate, endDate, individualId, dbObject)
            dbObject.insertPerformance(individualId, performance, rankingWalkforward)

        # Updating ranks in db
        resultPerformanceList = dbObject.getRankedIndividuals(rankingWalkforward)
        count = 0
        for individualId, dummy in resultPerformanceList:
            dbObject.insertRank(individualId, count+1, rankingWalkforward)
            count += 1
