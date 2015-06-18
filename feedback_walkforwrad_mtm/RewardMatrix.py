__author__ = 'Ciddhi'

from DBUtils import *
import numpy as np
import GlobalVariables as gv
from datetime import timedelta

class RewardMatrix:

    def computeRM (self, individualId, startDate, startTime, endDate, endTime, dbObject):

        # get net mtm and quantity for long and short trades
        resultPosMtm = dbObject.getTotalPosMTM(individualId, startDate, startTime, endDate, endTime)
        resultPosQty = dbObject.getTotalPosQty(individualId, startDate, startTime, endDate, endTime)
        resultNegMtm = dbObject.getTotalNegMTM(individualId, startDate, startTime, endDate, endTime)
        resultNegQty = dbObject.getTotalNegQty(individualId, startDate, startTime, endDate, endTime)
        posMtm = 0
        posQty = 0
        negMtm = 0
        negQty = 0

        # total the mtm and quantity for long and short trades
        for totalMtm, dummy in resultPosMtm:
            if totalMtm:
                posMtm = totalMtm
        for totalQty, dummy in resultPosQty:
            if totalQty:
                posQty = float(totalQty)
        for totalMtm, dummy in resultNegMtm:
            if totalMtm:
                negMtm = totalMtm
        for totalQty, dummy in resultNegQty:
            if totalQty:
                negQty = float(totalQty)

        posRm = np.zeros((3,3))
        negRm = np.zeros((3,3))

        # construct separate reward matrices for long and short trades and combine them linearly
        if (posQty>0):
            posRm = np.matrix([[posMtm/posQty*(posQty-2), posMtm/posQty*(posQty-1), posMtm],[posMtm/posQty*(posQty-1), posMtm, posMtm/posQty*(posQty+1)],[posMtm, posMtm/posQty*(posQty+1), posMtm/posQty*(posQty+2)]])
        if (negQty>0):
            negRm = np.matrix([[negMtm/negQty*(negQty-2), negMtm/negQty*(negQty-1), negMtm],[negMtm/negQty*(negQty-1), negMtm, negMtm/negQty*(negQty+1)],[negMtm, negMtm/negQty*(negQty+1), negMtm/negQty*(negQty+2)]])
        rm = gv.alpha * posRm + (1-gv.alpha) * negRm

        return rm

    def computeTrainingRM (self, individualId, startDate, startTime, endDate, endTime, dbObject):

        # get net mtm and quantity for long and short trades
        resultPosMtm = dbObject.getTrainingTotalPosMTM(individualId, startDate, startTime, endDate, endTime)
        resultPosQty = dbObject.getTrainingTotalPosQty(individualId, startDate, startTime, endDate, endTime)
        resultNegMtm = dbObject.getTrainingTotalNegMTM(individualId, startDate, startTime, endDate, endTime)
        resultNegQty = dbObject.getTrainingTotalNegQty(individualId, startDate, startTime, endDate, endTime)
        posMtm = 0
        posQty = 0
        negMtm = 0
        negQty = 0

        # total the mtm and quantity for long and short trades
        for totalMtm, dummy in resultPosMtm:
            if totalMtm:
                posMtm = totalMtm
        for totalQty, dummy in resultPosQty:
            if totalQty:
                posQty = float(totalQty)
        for totalMtm, dummy in resultNegMtm:
            if totalMtm:
                negMtm = totalMtm
        for totalQty, dummy in resultNegQty:
            if totalQty:
                negQty = float(totalQty)

        posRm = np.zeros((3,3))
        negRm = np.zeros((3,3))

        # construct separate reward matrices for long and short trades and combine them linearly
        if (posQty>0):
            posRm = np.matrix([[posMtm/posQty*(posQty-2), posMtm/posQty*(posQty-1), posMtm],[posMtm/posQty*(posQty-1), posMtm, posMtm/posQty*(posQty+1)],[posMtm, posMtm/posQty*(posQty+1), posMtm/posQty*(posQty+2)]])
        if (negQty>0):
            negRm = np.matrix([[negMtm/negQty*(negQty-2), negMtm/negQty*(negQty-1), negMtm],[negMtm/negQty*(negQty-1), negMtm, negMtm/negQty*(negQty+1)],[negMtm, negMtm/negQty*(negQty+1), negMtm/negQty*(negQty+2)]])
        rm = gv.alpha * posRm + (1-gv.alpha) * negRm

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

