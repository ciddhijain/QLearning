__author__ = 'Ciddhi'

from random import randint
from RewardMatrix import *
import numpy as np
import GlobalVariables as gv
from datetime import timedelta

class QMatrix:

    def calculateQMatrix(self, rewardMatrix, individualId, dbObject):
        qm = np.zeros((3,3))

        # fetch previous value of Q Matrix if it exists. Else initialize a new matrix
        resultQM = dbObject.getQMatrix(individualId)
        for row, column, qValue in resultQM:
            if row is not None:
                qm[row, column] = float(qValue)

        iterations = 0
        done = False

        qm_old = qm.copy()
        similarCount = 0            # variable keeps track of consecutive similar matrices

        # Do until convergence --> Check by cosine similarity / set difference / correlation
        while (True):
            initialState = randint(0,2)
            greedyLevel = 0

            # Maximize reward for future iterations
            while(greedyLevel<gv.maxGreedyLevel):
                action = randint(0,2)
                maxQValue = np.amax(qm, axis=1)[action]
                # Update q value
                qm[initialState,action] = rewardMatrix[initialState,action] + gv.gamma * maxQValue
                initialState = action
                greedyLevel = greedyLevel + 1
            iterations = iterations + 1

            # Check similarity between old and new Q Matrices
            if (self.checkSimilarityMatrices(qm, qm_old)):
                similarCount = similarCount + 1
                if similarCount>=5:
                    done = True
                    print('Converged in ------------------' + str(iterations) + ' iterations')
                    break
            else:
                similarCount = 0
                qm_old = qm.copy()

        # Update Q matrix for the individual in db
        dbObject.updateQMatrix(individualId, qm)

    # Function to check similarity in 2 matrices based on squared difference between corresponding values
    def checkSimilarityMatrices(self, m1, m2):
        diff = 0
        for i in range(0, 3, 1):
            for j in range (0, 3, 1):
                diff = diff + (m1[i,j]-m2[i,j])**2
        if diff<0.01:
            return True
        else:
            return False

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    rewardMatrixObject = RewardMatrix()
    qMatrixObject = QMatrix()

    startDate = 20120409
    startTime = timedelta(hours=10, minutes=30)
    endDate = 20120409
    endTime = timedelta(hours=11, minutes=30)

    resultIndividuals = dbObject.getIndividuals(startDate, startTime, endDate, endTime)
    for individualId, dummy in resultIndividuals:
        rewardMatrix = rewardMatrixObject.computeRM(individualId, startDate, startTime, endDate, endTime, dbObject)
        qMatrixObject.calculateQMatrix(rewardMatrix, individualId, dbObject)
        break
    dbObject.dbClose()