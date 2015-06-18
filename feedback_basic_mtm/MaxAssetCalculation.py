__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    resultAssets = dbObject.dbQuery("SELECT SUM(entry_qty*entry_price), entry_date FROM testing_tradesheet_table GROUP BY entry_date")
    maxSum = 0
    maxDate = None
    sumArr = []
    for sum, date in resultAssets:
        sumArr.append(sum)
        if sum>maxSum:
            maxSum = sum
            maxDate = date
    sumArr.sort()
    print(maxSum)
    print(maxDate)
    print(sumArr)