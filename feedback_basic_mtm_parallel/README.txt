The following steps are to be performed for running the code:
1. Create a new database and update variable 'databaseName' in file 'GlobalVariables.py'
2. Run file 'Setup.py'.
3. Import base tradesheet in table 'old_tradesheet_data_table' and price series in 'price_series_table'
4. Set values of other variables in 'GlobalVariables.py' as per requirement.
5. Run file 'WrapperQueueParallel.py'
6. Export tables 'tradesheet_data_table', 'asset_allocation_table', 'reallocation_table', 'asset_daily_allocation_table' before setting up another run on same DB.


Following are details about a few files:
1. Setup.py - This is used to set up the following database tables:
    a. old_tradesheet_data_table - this contains the original tradesheet
    b. price_series_table - this contains the price series
    c. tradesheet_data_table - this stores the new trades taken
    d. mtm_table - this stores the mtm for trades
    e. asset_allocation_table - this stores details for asset allocated to each individual.
                                individual_id=0 refers to dummy id which keeps track of overall asset deployed
    f. asset_daily_allocation_table - this stores free_asset at the end of each day
    g. reallocation_table - this stores details of every reallocation for individuals
    h. q_matrix_table - this stores q_matrix for every individual_id
2. DBUtils - This contains all queries that are used to fetch , update and insert in database. The function dbConnect contains username, password and other details that must be edited accordingly.
3. Global variables - This contains variables which are to be tested. Also, variables including maxThreads, maxTotalAsset, factor, alpha, gamma, unitQty, zeroRange
4. WrapperQueueParallel - This contains the wrapper code, which takes trades from original tradesheet, uses feedback information from MTM, reward matrix and q matrix and subsequently alters asset allocation for individuals

