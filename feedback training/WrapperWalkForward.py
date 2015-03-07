__author__ = 'Ciddhi'

from datetime import timedelta, datetime
from Reallocation import *
from RewardMatrix import *
from Training import *
from Live import *
from MTM import *
from Ranking import *
from QMatrix import *
from PerformanceMeasures import *
import calendar

if __name__ == "__main__":

    dbObject = DBUtils()
    rankingObject = Ranking()
    mtmObject = MTM()
    rewardMatrixObject = RewardMatrix()
    qMatrixObject = QMatrix()
    trainingObject = Training()
    liveObject = Live()

    reallocationObject = Reallocation()

    walkforwardStartDate = gv.startDate
    walkforwardEndDate = datetime(walkforwardStartDate.year, walkforwardStartDate.month, calendar.monthrange(walkforwardStartDate.year, walkforwardStartDate.month)[1]).date()
    trainingStartDate = walkforwardEndDate + timedelta(days=1)
    trainingEndDate = datetime(trainingStartDate.year, trainingStartDate.month, calendar.monthrange(trainingStartDate.year, trainingStartDate.month)[1]).date()
    liveStartDate = trainingEndDate + timedelta(days=1)
    liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
    periodEndDate = gv.endDate

    dbObject.resetRanks()
    dbObject.resetAssetAllocation(liveStartDate, startTime)
    done = False

    print('Started at : ' + datetime.now())
    while (not done):
        rankingObject.updateRankings(walkforwardStartDate, walkforwardEndDate, dbObject)
        trainingObject.train(trainingStartDate, trainingEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject)
        liveObject.live(liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
        if liveEndDate>=periodEndDate:
            done = True
        else:
            walkforwardStartDate = trainingStartDate
            walkforwardEndDate = trainingEndDate
            trainingStartDate = liveStartDate
            trainingEndDate = liveEndDate
            liveStartDate = trainingEndDate + timedelta(days=1)
            liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
            if liveEndDate>periodEndDate:
                liveEndDate = periodEndDate
    print('Finished at : ' + datetime.now())
