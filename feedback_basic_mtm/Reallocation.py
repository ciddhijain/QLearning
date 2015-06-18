__author__ = 'Ciddhi'

from DBUtils import *
import GlobalVariables as gv

class Reallocation:

    def reallocate(self, startDate, startTime, endDate, endTime, dbObject):
        resultIndividuals = dbObject.getIndividuals(startDate, startTime, endDate, endTime)
        posDeltaIndividuals = []
        negDeltaIndividuals = []
        noDeltaIndividuals = []


        for individualId, dummy1 in resultIndividuals:

            '''
            resultExists = dbObject.checkIndividualAssetExists(individualId)
            for exists, dummy2 in resultExists:
                if exists==0:
                    resultUsedAsset = dbObject.getUsedAsset(individualId)
                    for usedAsset, dummy3 in resultUsedAsset:
                        dbObject.addIndividualAsset(individualId, usedAsset)
                        dbObject.addNewState(individualId, startDate, startTime, 1)
            '''

            resultLastState = dbObject.getLastState(individualId)
            for lastState, individual in resultLastState:
                resultNextState = dbObject.getNextState(individualId, lastState)
                for nextState, dummy2 in resultNextState:
                    #print(nextState)
                    if nextState==0:
                        negDeltaIndividuals.append(individualId)
                    else:
                        if nextState==1:
                            noDeltaIndividuals.append(individualId)
                        else:
                            posDeltaIndividuals.append(individualId)
                    break
        #print(posDeltaIndividuals)
        #print(negDeltaIndividuals)

        for i in range(0, len(negDeltaIndividuals), 1):
            dbObject.reduceFreeAsset(negDeltaIndividuals[i], gv.unitQty)
            dbObject.addNewState(negDeltaIndividuals[i], endDate, endTime, 0)
        for i in range(0, len(posDeltaIndividuals), 1):
            dbObject.increaseFreeAsset(posDeltaIndividuals[i], gv.unitQty)
            dbObject.addNewState(posDeltaIndividuals[i], endDate, endTime, 2)
        for i in range(0, len(noDeltaIndividuals), 1):
            dbObject.addNewState(noDeltaIndividuals[i], endDate, endTime, 1)

        '''
        if len(negDeltaIndividuals)==0:
            print("All individuals doing well. No change in states")
            for i in range(0, len(posDeltaIndividuals), 1):
                dbObject.addNewState(posDeltaIndividuals[i], endDate, endTime, 1)
        else:
            if len(posDeltaIndividuals)>len(negDeltaIndividuals):
                scores = self.getScores(posDeltaIndividuals, startDate, startTime, endDate, endTime, dbObject)
                individualScores = list(zip(scores, posDeltaIndividuals))
                print(individualScores)
                individualScores.sort(key=lambda tup: tup[0])
                print(individualScores)
                for i in range(0,len(negDeltaIndividuals),1):
                    dbObject.reduceFreeAsset(negDeltaIndividuals[i], gv.unitQty)
                    dbObject.increaseFreeAsset(individualScores[i][1], gv.unitQty)
                    dbObject.addNewState(negDeltaIndividuals[i], endDate, endTime, 0)
                    dbObject.addNewState(individualScores[i][1], endDate, endTime, 2)
                for i in range(len(negDeltaIndividuals), len(posDeltaIndividuals), 1):
                    dbObject.addNewState(individualScores[i][1], endDate, endTime, 1)
            else:
                for i in range(0, len(negDeltaIndividuals), 1):
                    dbObject.reduceFreeAsset(negDeltaIndividuals[i], gv.unitQty)
                    dbObject.addNewState(negDeltaIndividuals[i], endDate, endTime, 0)
                for i in range(0, len(posDeltaIndividuals), 1):
                    dbObject.increaseFreeAsset(posDeltaIndividuals[i], gv.unitQty)
                    dbObject.addNewState(posDeltaIndividuals[i], endDate, endTime, 2)
                if len(posDeltaIndividuals)<len(negDeltaIndividuals):
                    dbObject.increaseFreeAsset(gv.dummyIndividualId, (len(negDeltaIndividuals)-len(posDeltaIndividuals))*gv.unitQty)
        '''


                # DO - Select some out of current strategies

    def getScores(self, individuals, startDate, startTime, endDate, endTime, dbObject):
        scores = []
        for individual in individuals:
            resultTime = dbObject.getStartTime(individual)
            for startDate, startTime in resultTime:
                resultPosMtm = dbObject.getTotalPosMTM(individual, startDate, startTime, endDate, endTime)
                resultPosQty = dbObject.getTotalPosQty(individual, startDate, startTime)
                resultNegMtm = dbObject.getTotalNegMTM(individual, startDate, startTime, endDate, endTime)
                resultNegQty = dbObject.getTotalNegQty(individual, startDate, startTime)
                posMtm = 0
                posQty = 0
                negMtm = 0
                negQty = 0
                for totalMtm, avgMtm in resultPosMtm:
                    posMtm = totalMtm
                    break
                for totalQty, avgQty in resultPosQty:
                    posQty = float(totalQty)
                    break
                for totalMtm, avgMtm in resultNegMtm:
                    negMtm = totalMtm
                    break
                for totalQty, avgQty in resultNegQty:
                    negQty = float(totalQty)
                    break
            score = gv.alpha * posMtm/posQty + (1 - gv.alpha) * negMtm/negQty
            scores.append(score)
        return scores

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    reallocationObject = Reallocation()
    reallocationObject.reallocate(dbObject)
    dbObject.dbClose()




