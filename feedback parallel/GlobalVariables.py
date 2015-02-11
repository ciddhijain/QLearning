__author__ = 'Ciddhi'

alpha = 0.6
gamma = 0.8
beta = 0.5
maxIterations = 50
maxGreedyLevel = 5
dummyIndividualId = 0               # This is to keep a track of max total capital that is invested in the portfolio
unitQty = 100000                    # This is the amount of each decrement in asset
hourWindow = 1                      # This is the window after which re-allocation is done
maxTotalAsset = 4000000            # This is the total asset deployed
factor = 8
maxAsset = maxTotalAsset/factor     # This is the maximum asset an individual can use
zeroRange = 0.005
aggregationUnit = 1
maxThreads = 4