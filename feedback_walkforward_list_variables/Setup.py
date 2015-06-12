__author__ = 'Ciddhi'

from DBUtils import *

class Setup:

    alpha = None
    gamma = None
    individualFactor = None
    zeroRange = None
    greedyLevel = None

    def __init__(self, alpha, gamma, individualFactor, zeroRange, greedyLevel):
        self.alpha = alpha
        self.gamma = gamma
        self.individualFactor = individualFactor
        self.zeroRange = zeroRange
        self.greedyLevel = greedyLevel

    def createTables(self, dbObject):
        global alpha
        global gamma
        global individualFactor
        global zeroRange
        global greedyLevel

        latestIndividualTable = gv.latestIndividualTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        trainingTradesheetTable = gv.trainingTradesheetTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        trainingAssetTable = gv.trainingAssetTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        rankingTable = gv.rankingTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        dailyAssetTable = gv.dailyAssetTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        newTradesheetTable = gv.newTradesheetTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        assetTable = gv.assetTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        qMatrixTable = gv.qMatrixTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        reallocationTable = gv.reallocationTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)
        performanceTable = gv.performanceTableBase + "_" + str(alpha) + "_" + str(gamma) + "_" + str(individualFactor) + "_" + str(zeroRange) + "_" + str(greedyLevel)

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + latestIndividualTable +
                         " ("
                         " individual_id int"
                         " )")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + trainingTradesheetTable +
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

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + trainingAssetTable +
                         " ("
                         " individual_id int,"
                         " total_asset decimal(15,4),"
                         " used_asset decimal(15,4),"
                         " free_asset decimal(15,4)"
                         " )")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + rankingTable +
                         " ("
                         " individual_id int,"
                         " ranking int"
                         " )")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + dailyAssetTable +
                         "("
                         "date date,"
                         "time time,"
                         "total_asset decimal(15,4)"
                         ")")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + newTradesheetTable +
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

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + assetTable +
                         " ("
                         " individual_id int,"
                         " total_asset decimal(15,4),"
                         " used_asset decimal(15,4),"
                         " free_asset decimal(15,4)"
                         " )")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + qMatrixTable +
                         " ("
                         " individual_id int,"
                         " row_num int,"
                         " column_num int,"
                         " q_value decimal(20,10)"
                         " )")

        dbObject.dbQuery("CREATE TABLE IF NOT EXISTS " + reallocationTable +
                         " ("
                         " individual_id int,"
                         " last_reallocation_date date,"
                         " last_reallocation_time time,"
                         " last_state int"
                         " )")

        dbObject.dbQuery(" CREATE TABLE IF NOT EXISTS " + performanceTable +
                         " ("
                         " individual_id int,"
                         " performance float"
                         " )")

        #return [latestIndividualTable, trainingTradesheetTable, trainingAssetTable, rankingTable, performanceTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable]
        return