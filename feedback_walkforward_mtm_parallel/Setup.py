__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS latest_individual_table"
                     " ("
                     " individual_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_mtm_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_asset_allocation_table"
                     " ("
                     " individual_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS ranking_table"
                     " ("
                     " individual_id int,"
                     " ranking int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_daily_allocation_table"
                     "("
                     "date date,"
                     "time time,"
                     "total_asset decimal(15,4)"
                     ")")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS mtm_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_allocation_table"
                     " ("
                     " individual_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS q_matrix_table"
                     " ("
                     " individual_id int,"
                     " row_num int,"
                     " column_num int,"
                     " q_value decimal(20,10)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS reallocation_table"
                     " ("
                     " individual_id int,"
                     " last_reallocation_date date,"
                     " last_reallocation_time time,"
                     " last_state int"
                     " )")

    dbObject.dbQuery(" CREATE TABLE IF NOT EXISTS performance_table"
                     " ("
                     " individual_id int,"
                     " performance float"
                     " )")

    dbObject.dbClose()
