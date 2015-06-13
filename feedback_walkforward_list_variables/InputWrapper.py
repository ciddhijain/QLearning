__author__ = 'Ciddhi'

import GlobalVariables as gv
from QLearningWrapper import *

if __name__ == "__main__":
    alphaList = gv.alpha
    gammaList = gv.gamma
    individualFactorList = gv.individualFactor
    zeroRangeList = gv.zeroRange
    greedyLevelList = gv.maxGreedyLevel

    qLearningObject = QLearningWrapper()

    for alpha in alphaList:
        for gamma in gammaList:
            for individualFactor in individualFactorList:
                for zeroRange in zeroRangeList:
                    for greedyLevel in greedyLevelList:
                        qLearningObject.feedback(alpha, gamma, individualFactor, zeroRange, greedyLevel)