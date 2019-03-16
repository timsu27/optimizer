import math
import numpy as np

ADAPTER = {
    "Amobee":{
        "adset_id":"package_id",
        "campaign_id":"ioid",
        "adset_progress":"package_progress",
        "campaign_progress":"io_progress"
    },
    "Facebook":{
        "adset_id":"adset_id",
        "campaign_id":"campaign_id",
        "adset_progress":"adset_progress",
        "campaign_progress":"campaign_progress"
    }
}

BID = 'bid'
INIT_BID = 'init_bid'
LAST_BID = 'last_bid'

CENTER = 1
WIDTH = 10
BID_RANGE = 0.8

def reverse_bid_amount(bid_amount):
    init_bid = bid_amount / ( BID_RANGE * ( normalized_sigmoid_fkt(CENTER, WIDTH, 0) - 0.5 ) + 1 )
    return init_bid

def normalized_sigmoid_fkt(center, width, progress):
    s= 1/( 1 + np.exp( width * ( progress-center ) ) )
    return s

def adjust(media, **status):
#     adset_id = status.get(ADSET_ID)
    init_bid = status.get(INIT_BID)
    last_bid = status.get(LAST_BID)
    ADSET_PROGRESS = ADAPTER[media].get("adset_progress")
    CAMPAIGN_PROGRESS = ADAPTER[media].get("campaign_progress")
    ADSET_ID = ADAPTER[media].get("adset_id")
    
    adset_progress = status.get(ADSET_PROGRESS)
    campaign_progress = status.get(CAMPAIGN_PROGRESS)
    
    if adset_progress > 1 and campaign_progress > 1:
        bid = math.ceil(init_bid)
    elif adset_progress > 1 and campaign_progress < 1:
        bid = last_bid
    else:
        bid = init_bid + BID_RANGE*init_bid*( normalized_sigmoid_fkt(CENTER, WIDTH, adset_progress) - 0.5 )
        bid = bid.astype(dtype=object)
    if not str(adset_progress).split(".")[0].isdigit():
        bid = init_bid
    print( { ADAPTER[media].get("adset_id"):status.get(ADSET_ID), BID:bid } )
    return { ADAPTER[media].get("adset_id"):status.get(ADSET_ID), BID:bid }
    return { ADSET_ID:adset_id, BID:bid }

if __name__=='__main__':
    status = {'campaign_progress': -0.0, 'adset_id': 23843355587230564, 'init_bid': 11, 'adset_progress': -0.7373064458048254, 'last_bid': 15}
    media = "Facebook"
    status = {'package_progress': 0.0, 'io_progress': 0.0, 'package_id': 1605818545, 'last_bid': 450, 'init_bid': 450}
    media = 'Amobee'
    adjust(media, **status)