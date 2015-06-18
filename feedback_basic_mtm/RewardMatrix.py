__author__ = 'Ciddhi'

from DBUtils import *
import numpy as np
import GlobalVariables as gv
from datetime import timedelta

class RewardMatrix:

    def computeRM (self, individualId, startDate, startTime, endDate, endTime, dbObject):
        resultPosMtm = dbObject.getTotalPosMTM(individualId, startDate, startTime, endDate, endTime)
        resultPosQty = dbObject.getTotalPosQty(individualId, startDate, startTime, endDate, endTime)
        resultNegMtm = dbObject.getTotalNegMTM(individualId, startDate, startTime, endDate, endTime)
        resultNegQty = dbObject.getTotalNegQty(individualId, startDate, startTime, endDate, endTime)
        posMtm = 0
        posQty = 0
        negMtm = 0
        negQty = 0
        for totalMtm, dummy in resultPosMtm:
            if totalMtm:
                posMtm = totalMtm
                #print('posMTM' + str(posMtm))
            break
        for totalQty, dummy in resultPosQty:
            if totalQty:
                posQty = float(totalQty)
                #print('posQty' + str(posQty))
            break
        for totalMtm, dummy in resultNegMtm:
            if totalMtm:
                negMtm = totalMtm
                #print('negMTM' + str(negMtm))
            break
        for totalQty, dummy in resultNegQty:
            if totalQty:
                negQty = float(totalQty)
                #print('negQty' + str(negQty))
            break
        posRm = np.zeros((3,3))
        negRm = np.zeros((3,3))
        if (posQty>0):
            posRm = np.matrix([[posMtm/posQty*(posQty-2), posMtm/posQty*(posQty-1), posMtm],[posMtm/posQty*(posQty-1), posMtm, posMtm/posQty*(posQty+1)],[posMtm, posMtm/posQty*(posQty+1), posMtm/posQty*(posQty+2)]])
        if (negQty>0):
            negRm = np.matrix([[negMtm/negQty*(negQty-2), negMtm/negQty*(negQty-1), negMtm],[negMtm/negQty*(negQty-1), negMtm, negMtm/negQty*(negQty+1)],[negMtm, negMtm/negQty*(negQty+1), negMtm/negQty*(negQty+2)]])
        rm = gv.alpha * posRm + (1-gv.alpha) * negRm
        #print('RM')
        #print(rm)
        return rm

if __name__ == "__main__":
    dbObject = DBUtils()
    rewardMatrixObject = RewardMatrix()

    startDate = 20120409
    startTime = timedelta(hours=9, minutes=15)
    endDate = 20120409
    endTime = timedelta(hours=10, minutes=30)
    dbObject.dbConnect()

    resultIndividuals = dbObject.getIndividuals(startDate, startTime, endDate, endTime)

    for individualId, dummy in resultIndividuals:
        print(individualId)
        rewardMatrixObject.computeRM(individualId, startDate, startTime, endDate, endTime, dbObject)

    dbObject.dbClose()

