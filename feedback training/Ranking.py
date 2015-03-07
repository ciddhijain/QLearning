__author__ = 'Ciddhi'

class Ranking:

    def updateRankings(self, startDate, endDate, dbObject):
        resultIndividuals = dbObject.getRefIndividuals(startDate, endDate)
        individualList = []
        performanceList = []

        # fetching performance for all individuals
        for individualId, dummy1 in resultIndividuals:
            resultLongPL = dbObject.getIndividualLongNetPL(startDate, endDate, individualId)
            resultShortPL = dbObject.getIndividualShortNetPL(startDate, endDate, individualId)
            resultDD = dbObject.getIndividualDrawdown(startDate, endDate, individualId)
            netPL = 0
            drawdown = 0.000001                                     # Small drawdown for initialization
            for pl, dummy2 in resultLongPL:
                if pl:
                    netPL += pl
            for pl, dummy2 in resultShortPL:
                if pl:
                    netPL += pl
            for dd, dummy2 in resultDD:
                if dd:
                    drawdown = dd
            individualList.append(individualId)
            performanceList.append(netPL/drawdown)

        # Sorting the individuals according to performance
        individualPerformance = list(zip(individualList, performanceList))
        individualPerformance.sort(key=lambda tup: tup[1])

        # Updating ranks in db
        for i in range(0, len(individualList), 1):
            dbObject.updateRank(individualPerformance[i][0], i+1)


