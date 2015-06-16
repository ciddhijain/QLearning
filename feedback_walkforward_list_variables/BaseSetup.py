__author__ = 'Ciddhi'

from DBUtils import *
import GlobalVariables as gv

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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + gv.dailyMtmTableBase +
                     " ("
                     " individual_id int,"
                     " mtm float,"
                     " mtm_date date"
                     " )")

    dbObject.dbQuery(" CREATE TABLE IF NOT EXISTS " + gv.performanceTableBase +
                     " ("
                     " individual_id int,"
                     " performance float,"
                     " ranking_walkforward_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + gv.rankingTableBase +
                     " ("
                     " individual_id int,"
                     " ranking int,"
                     " ranking_walkforward_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + gv.rankingWalkforwardTableBase +
                     " ("
                     " ranking_walkforward_id int,"
                     " ranking_start_date date,"
                     " ranking_end_date date"
                     " )")

    dbObject.dbClose()