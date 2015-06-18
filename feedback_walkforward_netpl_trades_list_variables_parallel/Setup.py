__author__ = 'Ciddhi'

from DBUtils import *

class Setup:

    alpha = None
    gamma = None
    individualFactor = None
    zeroRange = None
    greedyLevel = None

    def __init__(self, alpha_local, gamma_local, individualFactor_local, zeroRange_local, greedyLevel_local):
        global alpha
        global gamma
        global individualFactor
        global zeroRange
        global greedyLevel
        alpha = alpha_local
        gamma = gamma_local
        individualFactor = individualFactor_local
        zeroRange = zeroRange_local
        greedyLevel = greedyLevel_local

    def createQLearningTables(self):
        global alpha
        global gamma
        global individualFactor
        global zeroRange
        global greedyLevel

        dbObject = DBUtils()
        dbObject.dbConnect()

        variableString = "_a_" + str(alpha).replace('.', '_') + "_g_" + str(gamma).replace('.', '_') + "_f_" + str(individualFactor).replace('.', '_') + "_r_" + str(zeroRange).replace('.', '_') + "_l_" + str(greedyLevel).replace('.', '_')

        latestIndividualTable = gv.latestIndividualTableBase + variableString
        trainingTradesheetTable = gv.trainingTradesheetTableBase + variableString
        trainingAssetTable = gv.trainingAssetTableBase + variableString
        dailyAssetTable = gv.dailyAssetTableBase + variableString
        newTradesheetTable = gv.newTradesheetTableBase + variableString
        assetTable = gv.assetTableBase + variableString
        qMatrixTable = gv.qMatrixTableBase + variableString
        reallocationTable = gv.reallocationTableBase + variableString

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

        dbObject.dbClose()

        return [variableString, latestIndividualTable, trainingTradesheetTable, trainingAssetTable, qMatrixTable, reallocationTable, assetTable, dailyAssetTable, newTradesheetTable]