__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":

    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS old_tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS price_series_table"
                     " ("
                     " date date,"
                     " time time,"
                     " price float"
                     " )")

    dbObject.dbClose()