#!/usr/bin/env python
# coding: utf-8

# In[1]:

import math
from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.targeting import Targeting
from facebook_business.api import FacebookRequest

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'


import json
import requests
import pandas as pd
import datetime
import copy
import os

from facebook_datacollector import Campaigns
from facebook_datacollector import Field
from facebook_datacollector import DatePreset
from facebook_adapter import FacebookCampaignAdapter
from facebook_adapter import FacebookAdSetAdapter
import mysql_adactivity_save
from bid_operator import *
import math

campaign_objective = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion',
}
DATABASE = 'dev_facebook_test'
DATE = datetime.datetime.now().date()# - datetime.timedelta(1)
ACTION_BOUNDARY = 0.8
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

def copy_adset(adset_id):
    request = FacebookRequest(
            node_id=adset_id,
            method='POST',
            endpoint='/copies',
            api_type='EDGE',
    )
    params = {
            'deep_copy': True,
    }
    request.add_params(params)
#     try:
    response = request.execute()
    response_json = response.json()
#     print(response_json)
    new_adset_id = response_json.get('copied_adset_id')
#     print(new_adset_id)
    return new_adset_id
#     except:
#         pass


def modify_exists_adset(adset_id, adset_params):
    adset = AdSet(adset_id)
    
    for key in adset_params:
#         print(key, adset_params[key])
        adset[key] = adset_params[key]
    #print('[modify_exists_adset] adset:' , str(adset))
    update_response = adset.update()
    #print('update_response:' , update_response)
    remote_update_response = adset.remote_update()
    #print('remote_update_response:' , remote_update_response)
def check_init_bid(init_bid):
    if init_bid == None:
        return
    if init_bid > 100:
        init_bid = math.ceil( init_bid*1.1 )
    else:
        init_bid = init_bid + 1
    return init_bid
    
def config_adset_params_by_age(new_adset_id, age_max, age_min, init_bid=None):

    fields_adSet = [  AdSet.Field.campaign_id,  AdSet.Field.name, AdSet.Field.bid_amount, AdSet.Field.bid_strategy, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.bid_info, AdSet.Field.pacing_type
                    , AdSet.Field.attribution_spec, AdSet.Field.targeting]
    ad_set = AdSet(new_adset_id)
    ad_set_data = ad_set.remote_read(fields = fields_adSet)
    
    target = ad_set_data[AdSet.Field.targeting]
    original_name = ad_set_data[AdSet.Field.name]
                     
    target['age_max'] =  age_max
    target['age_min'] =  age_min
    
    if init_bid is None:
        adset_params = {
            AdSet.Field.name: original_name + ' ' + str(age_min) + '-' + str(age_max) ,
            AdSet.Field.targeting: target,
            'status': AdSet.Status.active,
        }
        ad_groups = ad_set.get_ads(fields=[
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.configured_status,
        ])
    else:
        adset_params = {
            AdSet.Field.name: original_name + ' ' + str(age_min) + '-' + str(age_max) + ' ' + str('init') + ' ' + str(init_bid) ,
            AdSet.Field.targeting: target,
            AdSet.Field.bid_amount: init_bid,
            'status': AdSet.Status.active,
        }
        ad_groups = ad_set.get_ads(fields=[
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.bid_amount,
            AdSet.Field.configured_status,
        ])
    for ad in ad_groups:
        ad_id = ad['id']
#         print('ad_id' , ad_id)
        ad_object = Ad(ad_id)
        ad_object['status'] = Ad.Status.active
        ad_object.remote_update()
    print(new_adset_id, init_bid)
    return adset_params

def async_copy_adset(adset_id_which_want_copy):
    url = "https://graph.facebook.com/v3.2/{id}/copies".format(id=adset_id_which_want_copy)
    payload = {
        "asyncbatch":[
            {
                "method":"POST",
                "relative_url":"{id}/copies".format(id=adset_id_which_want_copy),
                "name":"copy_adset_2",
                "body":"deep_copy=true&status_option=ACTIVE"
#                 "body":"status_option=ACTIVE"
            }
        ],
        "access_token":my_access_token
    }
    r = requests.post(url, json=payload)
    session_id = json.loads(r.text)['async_sessions'][0]['id']
    copied_adset_id = retrieve_copied_adset_id(session_id)
    return copied_adset_id

def retrieve_copied_adset_id(session_id):
    url = "https://graph.facebook.com/v3.2/{id}".format( id=session_id )
    headers = { "Authorization":"OAuth {}".format( my_access_token ) }
    payload = { "fields":"result" }
    r = requests.get(url, headers=headers, params=payload)
    while not bool( json.loads( json.loads( r.text )['result'] ) ):
        r = requests.get (url, headers=headers, params=payload )
    copied_adset_id = json.loads( json.loads( r.text )['result'] )['copied_adset_id']
    return copied_adset_id

def duplicate_asset_by_more_target(adset_id_which_want_copy,
                                   init_bid=None,
                                   bid_adjust=False,
                                   split_age=False):
    # duplicate more age target , use original age interval
    ad_set = AdSet(adset_id_which_want_copy)
    ad_set_data = ad_set.remote_read(fields = [AdSet.Field.targeting])
    target = ad_set_data[AdSet.Field.targeting]
    age_max = target['age_max']
    age_min = target['age_min']
    if split_age == True:
        interval_count = 2
    else:
        interval_count = 1
    print('split_age==', split_age, ' interval_count==', interval_count)
    age_interval = int((age_max - age_min)/interval_count)

    current_adset_min = age_min
    current_adset_max = current_adset_min + age_interval
    
    init_bid = check_init_bid(init_bid)
#     init_bid = math.ceil( init_bid + BID_RANGE*init_bid*( normalized_sigmoid_fkt(CENTER, WIDTH, 0) - 0.5 ) )
    
    for i in range(interval_count):
#         try:
#             print('[async copy success]')
#             new_adset_id = async_copy_adset(adset_id_which_want_copy) #async copy (not available)
#         except:
        new_adset_id = copy_adset(adset_id_which_want_copy) #sync copy 
        if new_adset_id is not None:
            
            if bid_adjust is True and init_bid is not None:
                adset_params = config_adset_params_by_age(new_adset_id, current_adset_max, current_adset_min, init_bid)
            else:
                adset_params = config_adset_params_by_age(new_adset_id, current_adset_max, current_adset_min)

            modify_exists_adset(new_adset_id, adset_params)

            current_adset_min += age_interval
            current_adset_max += age_interval

def check_adset_name(adset_id_which_want_copy):
    ad_set = AdSet(adset_id_which_want_copy)
    ad_set_data = ad_set.remote_read(fields = [AdSet.Field.name])
    name = ad_set_data[AdSet.Field.name]
    name_list = name.split( )
    for name in name_list:
        if name == 'Copy':
            return False
    return True
    
def duplicate_high_rank_adset(fb, campaign=None, charge_type=None, name_copy=False, split_age=False, bid_adjust=False):
    '''
    name_copy  : copy name with "Copy" or not
    split_age  : split age or not
    bid_adjust : adjust bid or not
    '''
    adset_list = get_sorted_adset(campaign)
    adset_for_copy, adset_for_off = split_adset_list(adset_list)
    print(adset_for_copy)
    print(adset_for_off)
    if name_copy == False:
        for i, adset_id in enumerate(adset_list):
            if check_adset_name(adset_id) is False:
                adset_list.remove(adset_id)

    if charge_type == 'CONVERSIONS':
#         for adset_id in adset_for_off:
#             update_status(adset_id, status=AdSet.Status.paused)
        for adset_id in adset_for_copy:
            s = FacebookAdSetAdapter( adset_id, fb )
            status = s.retrieve_adset_attribute()
            status['adset_progress'] = 0.0
            status['campaign_progress'] = 0.0
            try:
                init_bid = fb.init_bid_dict[ int(adset_id) ]
                init_bid = adjust("Facebook", **status)['bid']
            except:pass
            duplicate_asset_by_more_target( int(adset_id), init_bid, bid_adjust=True, split_age=False )
            update_status(adset_id, status=AdSet.Status.active)
    else:
        for adset_id in adset_list:
            s = FacebookAdSetAdapter( adset_id, fb )
            status = s.retrieve_adset_attribute()
            status['adset_progress'] = 0.0
            status['campaign_progress'] = 0.0
            try:
                init_bid = fb.init_bid_dict[ int(adset_id) ]
                init_bid = adjust("Facebook", **status)['bid']
            except:pass
            duplicate_asset_by_more_target( int(adset_id), init_bid, bid_adjust=True, split_age=True )
    return

def adjust_init_bid_of_adset(campaign, fb):
    adset_list = get_sorted_adset(campaign)
    
    for adset_id in adset_list:
        
        if check_adset_name(adset_id) is True:
#             init_bid = fb.init_bid_dict[ int(adset_id) ]
#             mysql_adactivity_save.update_init_bid( int(adset_id), init_bid )
            try:
                init_bid = fb.init_bid_dict[ int(adset_id) ]
                init_bid = check_init_bid(init_bid)
#                 print(campaign, '-'*30, + init_bid)
                mysql_adactivity_save.update_init_bid( int(adset_id), init_bid )
            except:
                print('pass')
                pass
    return

def get_sorted_adset(campaign):
    mydb=mysql_adactivity_save.connectDB(DATABASE)
#     try:
    df = pd.read_sql("select * from adset_score where campaign_id={}".format(campaign), con=mydb)
    df = df[df.request_time.dt.date==DATE].sort_values(by=['score'], ascending=False)
    adset_list = df['adset_id']
#     assert adset_list, 'Empty List'
#     except:
#         df_camp = mysql_adactivity_save.get_campaign_target(campaign)
#         charge_type = df_camp['charge_type'].iloc[0]
#         adset_list = Campaigns(campaign, charge_type).get_adsets()
    return adset_list

def main(campaign):
    df = mysql_adactivity_save.get_campaign_target(campaign)
    charge_type = df['charge_type'].iloc[0]

    daily_charge = df['daily_charge'].iloc[0]
    day_dict = Campaigns( campaign, charge_type ).generate_campaign_info(date_preset=DatePreset.yesterday)
    lifetime_dict = Campaigns( campaign, charge_type ).generate_campaign_info(date_preset=DatePreset.lifetime)
    try:target = int( day_dict['target'] )
    except:target = 0
    fb = FacebookCampaignAdapter(campaign)
    fb.retrieve_campaign_attribute()
    campaign_days_left = fb.campaign_days_left
    achieving_rate = target / daily_charge
#     print(target, daily_charge)
    print('[campaign_id]', campaign, '[achieving rate]', achieving_rate, target, daily_charge)
    bid_adjust=None
    if charge_type == 'CONVERSIONS':
        name_copy=True
        split_age=False
    else:
        name_copy=False
        split_age=True
        
    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        bid_adjust=False
            
    elif achieving_rate < ACTION_BOUNDARY:
        bid_adjust=True
    duplicate_high_rank_adset(fb, campaign=campaign, charge_type=charge_type, name_copy=name_copy, split_age=split_age, bid_adjust=bid_adjust)
#     try:
#         duplicate_high_rank_adset(fb, campaign=campaign, charge_type=charge_type, name_copy=split_age, split_age=split_age, bid_adjust=bid_adjust)
#         adjust_init_bid_of_adset(campaign, fb)
#     except:
#         pass
    return


def update_status(adset_id, status=AdSet.Status.active):
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.remote_update()
#     try:
#         adset.remote_update()
#     except:pass
    return

def check_status(adset_id):
    ad_set = AdSet(adset_id)
    ad_set_data = ad_set.remote_read(fields = [AdSet.Field.status])
    if ad_set_data.get(AdSet.Field.status) == 'ACTIVE':
        return 'ACTIVE'

def split_adset_list(adset_list):
    import math
    adset_list.to_list().sort(reverse=True)
    half = math.ceil( len(adset_list) / 2 )
    return adset_list[:half], adset_list[half:]


# In[14]:


if __name__=='__main__':
    import conversion_index_collector
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
#     df_camp = conversion_index_collector.get_campaign_target()    
#     for campaign_id in df_camp.campaign_id.unique():
#         main(campaign_id)
    
    main(23843301261480540)
    import gc
    gc.collect()





