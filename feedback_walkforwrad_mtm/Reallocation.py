__author__ = 'Ciddhi'

from DBUtils import *
import GlobalVariables as gv

class Reallocation:

    def reallocate(self, startDate, startTime, endDate, endTime, dbObject):

        # get all individuals which are active in last window
        resultIndividuals = dbObject.getIndividuals(startDate, startTime, endDate, endTime)
        posDeltaIndividuals = []
        negDeltaIndividuals = []
        noDeltaIndividuals = []

        for individualId, dummy1 in resultIndividuals:
            # get last state for the individual
            resultLastState = dbObject.getLastState(individualId)
            for lastState, individual in resultLastState:
                resultNextState = dbObject.getNextState(individualId, lastState)
                for nextState, dummy2 in resultNextState:
                    # Depending upon suggested next state, segregate individual_id
                    if nextState==0:
                        negDeltaIndividuals.append(individualId)
                    else:
                        if nextState==1:
                            noDeltaIndividuals.append(individualId)
                        else:
                            posDeltaIndividuals.append(individualId)

        # update asset and state for all individuals accordingly
        for i in range(0, len(negDeltaIndividuals), 1):
            dbObject.reduceFreeAsset(negDeltaIndividuals[i], gv.unitQty)
            dbObject.addNewState(negDeltaIndividuals[i], endDate, endTime, 0)
        for i in range(0, len(posDeltaIndividuals), 1):
            dbObject.increaseFreeAsset(posDeltaIndividuals[i], gv.unitQty)
            dbObject.addNewState(posDeltaIndividuals[i], endDate, endTime, 2)
        for i in range(0, len(noDeltaIndividuals), 1):
            dbObject.addNewState(noDeltaIndividuals[i], endDate, endTime, 1)


# To test
if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    reallocationObject = Reallocation()
    reallocationObject.reallocate(dbObject)
    dbObject.dbClose()




