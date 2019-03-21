#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import requests
import time
import pytz
import datetime
import math
from copy import deepcopy
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.targeting import Targeting
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi

import mysql_adactivity_save
from facebook_datacollector import Campaigns
from facebook_datacollector import DatePreset
from facebook_adapter import FacebookCampaignAdapter

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

DATABASE = 'dev_facebook_test'
DATE = datetime.datetime.now().date()
ACTION_BOUNDARY = 0.8
ACTION_DICT = {'bid':AdSet.Field.bid_amount, 'age':AdSet.Field.targeting, 'interest':AdSet.Field.targeting}

FIELDS = [
    AdSet.Field.id,
    AdSet.Field.account_id,
    AdSet.Field.adlabels,
    AdSet.Field.adset_schedule,
    AdSet.Field.attribution_spec,
    AdSet.Field.bid_amount,
    AdSet.Field.bid_info,
    AdSet.Field.bid_strategy,
    AdSet.Field.billing_event,
    AdSet.Field.budget_remaining,
    AdSet.Field.campaign_id,
    AdSet.Field.configured_status,
    AdSet.Field.created_time,
    AdSet.Field.creative_sequence,
    AdSet.Field.daily_budget,
    AdSet.Field.daily_min_spend_target,
    AdSet.Field.daily_spend_cap,
    AdSet.Field.destination_type,
    AdSet.Field.effective_status,
    AdSet.Field.end_time,
    AdSet.Field.frequency_control_specs,
    AdSet.Field.instagram_actor_id,
    AdSet.Field.is_dynamic_creative,
    AdSet.Field.issues_info,
    AdSet.Field.lifetime_budget,
    AdSet.Field.lifetime_imps,
    AdSet.Field.lifetime_min_spend_target,
    AdSet.Field.lifetime_spend_cap,
    AdSet.Field.name,
    AdSet.Field.optimization_goal,
    AdSet.Field.pacing_type,
    AdSet.Field.promoted_object,
    AdSet.Field.recommendations,
    AdSet.Field.recurring_budget_semantics,
    AdSet.Field.rf_prediction_id,
    AdSet.Field.start_time,
    AdSet.Field.status,
    AdSet.Field.targeting,
    AdSet.Field.time_based_ad_rotation_id_blocks,
    AdSet.Field.time_based_ad_rotation_intervals,
    AdSet.Field.updated_time,
#     AdSet.Field.use_new_app_click
]

def search_target_id(keyword=None):
    from facebook_business.adobjects.targetingsearch import TargetingSearch
    params = {
        'q': str(keyword),
        'type': TargetingSearch.TargetingSearchTypes.interest,
    }
    resp = TargetingSearch.search(params=params)
    current_resp = resp[0]
    return resp['id']

def get_ad_id_list(adset_id):
    ad_id_list = list()
    adset = AdSet(adset_id)
    ads = adset.get_ads(fields=[AdSet.Field.id])
    for ad in ads:
        ad_id_list.append(ad[Ad.Field.id])
    return ad_id_list

def retrieve_origin_adset_params(origin_adset_id):
    origin_adset = AdSet(fbid=origin_adset_id)
    origin_adset_params = origin_adset.remote_read(fields=FIELDS)
    return origin_adset_params

def assign_copied_ad_to_new_adset(new_adset_id=None, ad_id=None):
    url = "https://graph.facebook.com/v3.2/{}/copies".format(ad_id)
    querystring = {
        "adset_id":"{}".format(new_adset_id),
        "status_option":"INHERITED_FROM_SOURCE"}
    headers = {
        'Authorization': "Bearer {}".format(my_access_token),}
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response.text

def update_interest(adset_id=None, adset_params=None):
    adset = AdSet(adset_id)
    update_response = adset.update(adset_params)
    remote_update_response = adset.remote_update(
        params={'status': 'PAUSED',}
    )
    return



def get_sorted_adset(campaign):
    mydb=mysql_adactivity_save.connectDB(DATABASE)
    try:
        df = pd.read_sql("select * from adset_score where campaign_id=%s" %(campaign), con=mydb)
        df = df[df.request_time.dt.date==DATE].sort_values(by=['score'], ascending=False)
        adset_list = df['adset_id']
        assert adset_list, 'Empty List'
    except:
        df_camp = mysql_adactivity_save.get_campaign_target(campaign)
        charge_type = df_camp['charge_type'].iloc[0]
        adset_list = Campaigns(campaign, charge_type).get_adsets()
    return adset_list

def split_adset_list(adset_list):
    import math
    adset_list.sort(reverse=True)
    half = math.ceil( len(adset_list) / 2 )
    return adset_list[:half], adset_list[half:]



def check_adset_name(adset_name):
    adset_name_list = adset_name.split( )
    for adset_name in adset_name_list:
        if adset_name == 'Copy':
            return False
    return True
    
def check_init_bid(init_bid):
    if init_bid > 100:
        bid = math.ceil( init_bid*1.1 )
        return bid
    else:
        bid = init_bid + 1
        return bid


def copy_adset(adset_id, actions, adset_params=None):
    new_adset_params = adset_params
    origin_adset_name = adset_params[AdSet.Field.name]
    new_adset_params[AdSet.Field.id]=None
    for i, action in enumerate(actions.keys()):
        if action == 'bid':
            new_adset_params[ ACTION_DICT[action] ] = actions[action] # for bid

        elif action == 'age':
            age_list = actions[action][0].split('-')
            new_adset_params[AdSet.Field.targeting]["age_min"] = int(age_list[:1][0])
            new_adset_params[AdSet.Field.targeting]["age_max"] = int(age_list[1:][0])
            new_adset_params[AdSet.Field.name] = origin_adset_name + ' Copy - {}'.format(actions[action])
            
        elif action == 'interest':
            if actions[action] is None:
                new_adset_params[AdSet.Field.targeting]["flexible_spec"] = None
            else:
                new_adset_params[AdSet.Field.targeting]["flexible_spec"] = {"interests" : [actions[action]]}
                
    new_adset_id = make_adset(new_adset_params)
    new_adset_params[AdSet.Field.name] = origin_adset_name
    time.sleep(10)
    ad_id_list = get_ad_id_list(adset_id)
    for ad_id in ad_id_list:
        assign_copied_ad_to_new_adset(new_adset_id=new_adset_id,ad_id=ad_id)

def make_adset(adset_params):
    account_id = adset_params[AdSet.Field.account_id]
    new_adset = AdSet(parent_id='act_{}'.format(account_id))
    new_adset.update(adset_params)
    new_adset.remote_create(params={'status': 'ACTIVE',})
    return new_adset[AdSet.Field.id]

def main(campaign):
    df = mysql_adactivity_save.get_campaign_target(campaign)
    charge_type = df['charge_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    day_dict = Campaigns( campaign, charge_type ).generate_campaign_info(date_preset=DatePreset.today)
    lifetime_dict = Campaigns( campaign, charge_type ).generate_campaign_info(date_preset=DatePreset.lifetime)
    try:target = int( day_dict['target'] )
    except:target = 10000
    fb = FacebookCampaignAdapter(campaign)
    fb.get_df()
    fb.get_bid()
    fb.get_campaign_days_left()
    campaign_days_left = fb.campaign_days_left
    achieving_rate = target / daily_charge
    print('[campaign_id]', campaign, '[achieving rate]', achieving_rate, target, daily_charge)

    adset_list = get_sorted_adset(campaign)
    adset_for_copy, adset_for_off = split_adset_list(adset_list)
    ### get ready to duplicate
    actions = {'bid':None, 'age':list(), 'interest':None}
    actions_list = list()
    
    bid_adjust = False
    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        bid_adjust=False

    elif achieving_rate < ACTION_BOUNDARY:
        bid_adjust=True

    if charge_type == 'CONVERSIONS':
        copy_name_with_copy=True
        split_age=False
    elif charge_type == 'LINK_CLICKS':
        copy_name_with_copy=False
        split_age=True
    for adset_id in adset_for_copy:
        print(adset_for_copy)
        ### bid adjust
        bid = fb.init_bid_dict[int(adset_id)]
        if bid_adjust:
            bid = check_init_bid(bid)
        actions.update( {'bid':bid} )
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        origin_adset_params[AdSet.Field.id] = None
        origin_name = origin_adset_params[AdSet.Field.name]
        ### interest decision
        try:
            origin_interest = origin_adset_params[AdSet.Field.targeting]["flexible_spec"][0]
        except:
            origin_interest = None
        ### min max age
        adset_max = origin_adset_params[AdSet.Field.targeting]["age_max"]
        adset_min = origin_adset_params[AdSet.Field.targeting]["age_min"]
        
        try:
            actions['age'][0] = str(adset_min)+'-'+str(adset_max)
            actions.update( { 'interest':origin_interest['interests'][0] } )
        except:
            actions['age'].append( str(adset_min)+'-'+str(adset_max) )
            actions.update( { 'interest':None } )
        ### whether to split age or copy adset names with 'copy'
        if not copy_name_with_copy and check_adset_name(origin_name):
            if split_age:
                interval = 2
                age_interval = math.ceil( (adset_max-adset_min) / interval )
                for i in range(interval):
                    current_adset_min = adset_min
                    current_adset_max = current_adset_min + age_interval
                    actions['age'][0] = str(current_adset_min)+'-'+str(current_adset_max)
                    adset_min = current_adset_max
                    actions_copy = deepcopy(actions)
                    copy_adset(adset_id, actions_copy, origin_adset_params)
            else:
                actions_copy = deepcopy(actions)
                actions_list.append(actions_copy)
        else:
            actions['age'] = list()
            actions['age'].append( str(adset_min)+'-'+str(adset_max) )
            actions_copy = deepcopy(actions)
            copy_adset(adset_id, actions_copy, origin_adset_params)


if __name__=='__main__':
    import index_collector_conversion_facebook
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    df_camp = index_collector_conversion_facebook.get_campaign_target()    
    for campaign_id in df_camp.campaign_id.unique():
        main(campaign_id)


# In[ ]:


#get_ipython().system('jupyter')

