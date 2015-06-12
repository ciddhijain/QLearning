__author__ = 'Ciddhi'

from DBUtils import *
import numpy as np
import GlobalVariables as gv
from datetime import timedelta

class RewardMatrix:

    alpha = None

    def __init__(self, alpha):
        self.alpha = alpha

    def computeRM (self, mtmList):

        global alpha
        posMtm = mtmList[0]
        posQty = mtmList[1]
        negMtm = mtmList[2]
        negQty = mtmList[3]

        posRm = np.zeros((3,3))
        negRm = np.zeros((3,3))

        # construct separate reward matrices for long and short trades and combine them linearly
        if (posQty>0):
            posRm = np.matrix([[posMtm/posQty*(posQty-2), posMtm/posQty*(posQty-1), posMtm],
                               [posMtm/posQty*(posQty-1), posMtm, posMtm/posQty*(posQty+1)],
                               [posMtm, posMtm/posQty*(posQty+1), posMtm/posQty*(posQty+2)]])
        if (negQty>0):
            negRm = np.matrix([[negMtm/negQty*(negQty-2), negMtm/negQty*(negQty-1), negMtm],
                               [negMtm/negQty*(negQty-1), negMtm, negMtm/negQty*(negQty+1)],
                               [negMtm, negMtm/negQty*(negQty+1), negMtm/negQty*(negQty+2)]])
        rm = alpha * posRm + (1-alpha) * negRm

        return rm

    def computeTrainingRM (self, mtmList):

        global alpha
        posMtm = mtmList[0]
        posQty = mtmList[1]
        negMtm = mtmList[2]
        negQty = mtmList[3]

        posRm = np.zeros((3,3))
        negRm = np.zeros((3,3))

        # construct separate reward matrices for long and short trades and combine them linearly
        if (posQty>0):
            posRm = np.matrix([[posMtm/posQty*(posQty-2), posMtm/posQty*(posQty-1), posMtm],
                               [posMtm/posQty*(posQty-1), posMtm, posMtm/posQty*(posQty+1)],
                               [posMtm, posMtm/posQty*(posQty+1), posMtm/posQty*(posQty+2)]])
        if (negQty>0):
            negRm = np.matrix([[negMtm/negQty*(negQty-2), negMtm/negQty*(negQty-1), negMtm],
                               [negMtm/negQty*(negQty-1), negMtm, negMtm/negQty*(negQty+1)],
                               [negMtm, negMtm/negQty*(negQty+1), negMtm/negQty*(negQty+2)]])
        rm = alpha * posRm + (1-alpha) * negRm

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

