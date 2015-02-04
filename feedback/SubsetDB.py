__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    dbObject.dbQuery("CREATE TABLE testing_tradesheet_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " entry_qty int,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float"
                     " )")
    resultIndividuals = dbObject.dbQuery("SELECT count(*), individual_id FROM old_tradesheet_data_table WHERE entry_date>='2012-04-09'"
                                         " AND entry_date<='2012-07-09' GROUP BY individual_id ORDER BY COUNT(*) DESC")
    countNum = 1
    for count, individualId in resultIndividuals:
        if count>=100:
            print(str(countNum) + " - Inserting for individual : " + str(individualId))
            dbObject.dbQuery("INSERT INTO testing_tradesheet_table SELECT * FROM old_tradesheet_data_table WHERE individual_id="
                             + str(individualId) + " AND entry_date>='2012-04-09' AND entry_date<='2012-07-09'")
            countNum = countNum + 1
        else:
            break
    dbObject.dbClose()

