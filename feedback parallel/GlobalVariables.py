__author__ = 'Ciddhi'

alpha = 0.6                         # This defines the weightage to long trades as compared to short trades while constructing reward matrix
gamma = 0.9                         # This defines the weightage of old data as compared to latest observations of reward matrix
maxGreedyLevel = 5
dummyIndividualId = 0               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 250000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 10000000            # This is the total asset deployed
factor = 10
maxAsset = maxTotalAsset/factor     # This is the maximum asset an individual can use
zeroRange = 0.005                   # This determines the spread between states 0, 1, 2
aggregationUnit = 1
maxThreads = 10                     # This is the4 maximum number of threads that can run concurrently