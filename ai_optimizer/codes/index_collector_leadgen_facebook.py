#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as adset
import facebook_business.adobjects.ad as ad
import facebook_business.adobjects.campaign as campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

import json
import datetime
import pandas as pd
from bid_operator import *
import math

# In[2]:


CAMPAIGN_OBJECTIVE_FIELD = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement',
    'VIDEO_VIEWS': 'video_view',
    'CONVERSIONS': 'offsite_conversion.fb_pixel_purchase',
    'APP_INSTALLS': 'app_installs',
    'BRAND_AWARENESS': 'brand_awareness',
    'EVENT_RESPONSES': 'event_responses',
    'LEAD_GENERATION': 'leadgen.other',
    'LOCAL_AWARENESS': 'local_awareness',
    'MESSAGES': 'messages',
    'OFFER_CLAIMS': 'offer_claims',
    'PAGE_LIKES': 'page_likes',
    'PRODUCT_CATALOG_SALES': 'product_catalog_sales',
    'REACH': 'reach',
    'ALL_CLICKS': 'clicks',
}
CAMPAIGN_FIELD = {
    'spend_cap': campaign.Campaign.Field.spend_cap,
    'objective': campaign.Campaign.Field.objective,
    'start_time': campaign.Campaign.Field.start_time,
    'stop_time': campaign.Campaign.Field.stop_time,
}
ADSET_FIELD = {
    'bid_amount': adset.AdSet.Field.bid_amount,
    'daily_budget': adset.AdSet.Field.daily_budget
}
CAMPAIGN_INSIGHTS_FIELD = {
    'campaign_id': AdsInsights.Field.campaign_id,
}
ADSET_INSIGHTS_FIELD = {
    'adset_id': AdsInsights.Field.adset_id,
}
GENERAL_FIELD = {
    'impressions': AdsInsights.Field.impressions,
    'reach': AdsInsights.Field.reach,
    'spend': AdsInsights.Field.spend,
    'cpc': AdsInsights.Field.cpc,
    'clicks': AdsInsights.Field.clicks,
}
TARGET_FIELD = {
    'actions': AdsInsights.Field.actions,
    'cost_per_actions': AdsInsights.Field.cost_per_action_type,
}
LEAD_METRICS = {
    'fb_pixel_complete_registration':'fb_pixel_complete_registration',
    'fb_pixel_lead':'fb_pixel_lead',
    'leadgen.other':'leadgen.other',
    'offsite_conversion.fb_pixel_view_content': 'fb_pixel_view_content',
    'landing_page_view': 'landing_page_view',
    'link_click': 'link_click'
}

# In[5]:


class Field:
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
    period = 'period'
    daily_budget = 'daily_budget'
    account_id = 'account_id'
    actions = 'actions'
    adset_id = 'adset_id'
    campaign_id = 'campaign_id'
    clicks = 'clicks'
    targeting = 'targeting'
    age_max = 'age_max'
    age_min = 'age_min'
    flexible_spec = 'flexible_spec'
    geo_locations = 'geo_locations'
    conversion_values = 'conversion_values'
    conversions = 'conversions'
    leadgen_other = 'leadgen.other'
    cost_per_leadgen_other = 'cost_per_leadgen.other'
    purchase = 'purchase'
    cost_per_purchase = 'cost_per_purchase'
    cost_per_10_sec_video_view = 'cost_per_10_sec_video_view'
    cost_per_15_sec_video_view = 'cost_per_15_sec_video_view'
    cost_per_2_sec_continuous_video_view = 'cost_per_2_sec_continuous_video_view'
    cost_per_action_type = 'cost_per_action_type'
    cost_per_ad_click = 'cost_per_ad_click'
    cost_per_conversion = 'cost_per_conversion'
    cost_per_dda_countby_convs = 'cost_per_dda_countby_convs'
    cost_per_estimated_ad_recallers = 'cost_per_estimated_ad_recallers'
    cost_per_inline_link_click = 'cost_per_inline_link_click'
    cost_per_inline_post_engagement = 'cost_per_inline_post_engagement'
    cost_per_one_thousand_ad_impression = 'cost_per_one_thousand_ad_impression'
    cost_per_outbound_click = 'cost_per_outbound_click'
    cost_per_thruplay = 'cost_per_thruplay'
    cost_per_unique_action_type = 'cost_per_unique_action_type'
    cost_per_unique_click = 'cost_per_unique_click'
    cost_per_unique_conversion = 'cost_per_unique_conversion'
    cost_per_unique_inline_link_click = 'cost_per_unique_inline_link_click'
    cost_per_unique_outbound_click = 'cost_per_unique_outbound_click'
    cpc = 'cpc'
    cpm = 'cpm'
    cpp = 'cpp'
    created_time = 'created_time'
    ctr = 'ctr'
    frequency = 'frequency'
    frequency_value = 'frequency_value'
    impressions = 'impressions'
    inline_link_click_ctr = 'inline_link_click_ctr'
    inline_link_clicks = 'inline_link_clicks'
    inline_post_engagement = 'inline_post_engagement'
    objective = 'objective'
    outbound_clicks = 'outbound_clicks'
    outbound_clicks_ctr = 'outbound_clicks_ctr'
    product_id = 'product_id'
    purchase_roas = 'purchase_roas'
    reach = 'reach'
    relevance_score = 'relevance_score'
    spend = 'spend'
    spend_cap = 'spend_cap'
    unique_actions = 'unique_actions'
    unique_clicks = 'unique_clicks'
    unique_conversions = 'unique_conversions'
    unique_ctr = 'unique_ctr'
    unique_inline_link_click_ctr = 'unique_inline_link_click_ctr'
    unique_inline_link_clicks = 'unique_inline_link_clicks'
    unique_link_clicks_ctr = 'unique_link_clicks_ctr'
    unique_outbound_clicks = 'unique_outbound_clicks'
    unique_outbound_clicks_ctr = 'unique_outbound_clicks_ctr'
    unique_video_continuous_2_sec_watched_actions = 'unique_video_continuous_2_sec_watched_actions'
    unique_video_view_10_sec = 'unique_video_view_10_sec'
    unique_video_view_15_sec = 'unique_video_view_15_sec'
    video_10_sec_watched_actions = 'video_10_sec_watched_actions'
    video_15_sec_watched_actions = 'video_15_sec_watched_actions'
    video_30_sec_watched_actions = 'video_30_sec_watched_actions'
    video_avg_percent_watched_actions = 'video_avg_percent_watched_actions'
    video_avg_time_watched_actions = 'video_avg_time_watched_actions'
    video_continuous_2_sec_watched_actions = 'video_continuous_2_sec_watched_actions'
    video_p100_watched_actions = 'video_p100_watched_actions'
    video_p25_watched_actions = 'video_p25_watched_actions'
    video_p50_watched_actions = 'video_p50_watched_actions'
    video_p75_watched_actions = 'video_p75_watched_actions'
    video_p95_watched_actions = 'video_p95_watched_actions'
    video_play_actions = 'video_play_actions'
    video_play_retention_0_to_15s_actions = 'video_play_retention_0_to_15s_actions'
    video_play_retention_20_to_60s_actions = 'video_play_retention_20_to_60s_actions'
    video_play_retention_graph_actions = 'video_play_retention_graph_actions'
    video_thruplay_watched_actions = 'video_thruplay_watched_actions'
    video_time_watched_actions = 'video_time_watched_actions'
    website_ctr = 'website_ctr'
    website_purchase_roas = 'website_purchase_roas'
    
class DatePreset:
    today = 'today'
    yesterday = 'yesterday'
    this_month = 'this_month'
    last_month = 'last_month'
    this_quarter = 'this_quarter'
    lifetime = 'lifetime'
    last_3d = 'last_3d'
    last_7d = 'last_7d'
    last_14d = 'last_14d'
    last_28d = 'last_28d'
    last_30d = 'last_30d'
    last_90d = 'last_90d'
    last_week_mon_sun = 'last_week_mon_sun'
    last_week_sun_sat = 'last_week_sun_sat'
    last_quarter = 'last_quarter'
    last_year = 'last_year'
    this_week_mon_today = 'this_week_mon_today'
    this_week_sun_today = 'this_week_sun_today'
    this_year = 'this_year'
    


# In[6]:


class Campaigns(object):
    def __init__( self, campaign_id, charge_type ):
        self.campaign_id = campaign_id
        self.charge_type = charge_type
        self.campaign_insights = dict()
        self.campaign_features = dict()
        self.campaign_info = dict()
        
    # Getters
    
    def get_campaign_features( self ):
        ad_campaign =campaign.Campaign( self.campaign_id )
        adcamps = ad_campaign.remote_read( fields=list(CAMPAIGN_FIELD.values()) )
        for campaign_field in list(adcamps.keys()):
            self.campaign_features.update( {campaign_field:adcamps.get(campaign_field)} )
        return self.campaign_features
        
    def get_campaign_insights( self, date_preset=None ):
        campaigns = campaign.Campaign( self.campaign_id )
        params = {
            'date_preset': date_preset,
        }
        insights = campaigns.get_insights(
            params=params,
            fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
        )
        if len(insights) > 0:
            current_campaign = insights[0]
            if current_campaign.get(Field.impressions):
                spend = current_campaign.get( Field.spend )
                impressions = current_campaign.get( Field.impressions )
                self.campaign_insights.update( { Field.spend: int(spend) } )
                self.campaign_insights.update( { Field.impressions: int(impressions) } )
                try:
                    for act in current_campaign.get( Field.actions ):
                        if act["action_type"] in LEAD_METRICS:
                            self.campaign_insights.update( {
                                LEAD_METRICS[ act["action_type"] ]: int(act["value"])
                            } )
                            self.campaign_insights.update( {
                                'cost_per_' + LEAD_METRICS[ act["action_type"] ] : float(spend) / float(act["value"])
                            } )
                except Exception as e:
                    print('[conversion_index_collector.Campaigns.get_campaign_insights]', e)
            return self.campaign_insights
    
    def get_adsets( self ):
        adset_list=list()
        campaigns = campaign.Campaign( self.campaign_id )
        adsets = campaigns.get_ad_sets( fields = [ adset.AdSet.Field.id ])
        ads = campaigns.get_ad_sets( fields = [ adset.AdSet.Field.id ])
        for adset_id in adsets:
            adset_list.append( adset_id.get("id") )
        return adset_list
    
    def generate_campaign_info(self, date_preset=None):
        self.get_campaign_features()
        self.get_campaign_insights(date_preset=date_preset)
        
        self.campaign_features[ Field.campaign_id ] = self.campaign_features.pop('id')
        self.campaign_features[ Field.target_type ] = self.campaign_features.pop('objective')
        self.campaign_features[ Field.start_time ] = datetime.datetime.strptime( self.campaign_features[Field.start_time],'%Y-%m-%dT%H:%M:%S+%f' )
        ### for test
        try:
            self.campaign_features[ Field.stop_time ] = datetime.datetime.strptime( self.campaign_features[Field.stop_time],'%Y-%m-%dT%H:%M:%S+%f' )
        except:
            self.campaign_features[ Field.stop_time ] = datetime.datetime.now() + datetime.timedelta(1)
        ###
        self.campaign_features[ Field.period ] = ( self.campaign_features[Field.stop_time] - self.campaign_features[Field.start_time] ).days
        self.campaign_features[ Field.start_time ] = self.campaign_features[Field.start_time].strftime( '%Y-%m-%d %H:%M:%S' )
        self.campaign_features[ Field.stop_time ] = self.campaign_features[Field.stop_time].strftime( '%Y-%m-%d %H:%M:%S' )
        try:
            self.campaign_features[ Field.daily_budget ] = int( self.campaign_features[Field.spend_cap] )/self.campaign_features[Field.period]
        except:
            self.campaign_features[ Field.daily_budget ] = 50000/self.campaign_features[Field.period]
        
        
        self.campaign_info = { **self.campaign_insights, **self.campaign_features }
        return self.campaign_info


# In[7]:


class AdSets(object):
    def __init__( self, adset_id, charge_type ):
        self.adset_id = adset_id
        self.charge_type = charge_type
        self.adset_features = dict()
        self.adset_insights = dict()
        self.adset_info = dict()
        
    # Getters
        
    def get_adset_features( self ):
        adsets = adset.AdSet( self.adset_id )
        adsets = adsets.remote_read( fields=list( ADSET_FIELD.values() ) )
        for adset_field in list(adsets.keys()):
            self.adset_features.update( { adset_field:adsets.get(adset_field) } )
        return self.adset_features
    
    def get_adset_insights( self, date_preset=None ):
        adsets = adset.AdSet( self.adset_id )
        params = {
            'date_preset': date_preset,
        }
        insights = adsets.get_insights(
            params=params,
            fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
        )
        if len(insights) > 0:
            current_adset = insights[0]
            if current_adset.get(Field.impressions):
                spend = current_adset.get( Field.spend )
                impressions = current_adset.get( Field.impressions )
                self.adset_insights.update( { Field.spend: int(spend) } )
                self.adset_insights.update( { Field.impressions: int(impressions) } )
            try:
                for act in current_adset.get( Field.actions ):
                    if act["action_type"] in LEAD_METRICS:
                        self.adset_insights.update( {
                            LEAD_METRICS[ act["action_type"] ]: int(act["value"])
                        } )
                        self.adset_insights.update( {
                            'cost_per_' + LEAD_METRICS[ act["action_type"] ] : float(spend) / float(act["value"])
                        } )
            except Exception as e:
                print('[conversion_index_collector.AdSets.get_adset_insights]', e)
            finally:
                return self.adset_insights
        
    def generate_adset_info(self, date_preset=None):
        self.get_adset_features()
        self.get_adset_insights(date_preset=date_preset)
        self.adset_features[ Field.adset_id ] = self.adset_features.pop('id')
        self.adset_info = { **self.adset_insights, **self.adset_features }
        return self.adset_info


# In[8]:


def data_collect( campaign_id, total_clicks, charge_type ):
    camp = Campaigns( campaign_id, charge_type )
    life_time_campaign_status = camp.generate_campaign_info( date_preset=DatePreset.lifetime )
    stop_time = datetime.datetime.strptime( life_time_campaign_status[Field.stop_time],'%Y-%m-%d %H:%M:%S' )
    period_left = (stop_time-datetime.datetime.now()).days + 1
    try:
        target_left = int(total_clicks) - int(life_time_campaign_status[ Field.leadgen_other ])
        target_pair = {
            Field.leadgen_other: camp.campaign_insights[Field.leadgen_other],
            Field.cost_per_leadgen_other: camp.campaign_insights[Field.cost_per_leadgen_other]
        }
    except:
        target_left = int(total_clicks)
        target_pair = {
            Field.leadgen_other: 0,
            Field.cost_per_leadgen_other: 0
        }
    try:
        camp.campaign_insights[Field.spend]
        camp.campaign_insights[Field.impressions]
    except:
        camp.campaign_insights[Field.spend]=0
        camp.campaign_insights[Field.impressions]=0
    campaign_status = {
        'charge_type': charge_type,
        'destination': total_clicks,
        'target_left': target_left,
        'daily_charge': target_left / period_left,
    }
    target_pair[Field.target] = target_pair.pop(Field.leadgen_other)
    target_pair[Field.cost_per_target] = target_pair.pop(Field.cost_per_leadgen_other)
    campaign_dict = {
        **camp.campaign_features,
        **target_pair,
        **campaign_status,
        Field.spend:camp.campaign_insights[Field.spend],
        Field.impressions:camp.campaign_insights[Field.impressions]
    }
    df_camp = pd.DataFrame(campaign_dict, index=[0])
    df_camp[df_camp.columns] = df_camp[df_camp.columns].apply(pd.to_numeric, errors='ignore')
    campaign_metrics = {
        **camp.campaign_insights,
    }
    adset_list = camp.get_adsets()
    for adset_id in adset_list:
        adset = AdSets(adset_id, charge_type)
#         adset_dict = adset.generate_adset_info(date_preset=DatePreset.today)
        adset_dict = adset.generate_adset_info(date_preset=DatePreset.lifetime) #for testing
        adset_dict['request_time'] = datetime.datetime.now()
        adset_dict['campaign_id'] = campaign_id
        ### for test
        try:
            adset_dict['bid_amount'] = math.ceil(reverse_bid_amount(adset_dict['bid_amount']))
        except:
            adset_dict['bid_amount'] = 50
        ###
        df_adset = pd.DataFrame(adset_dict, index=[0])
        insertion("adset_leads_metrics", df_adset)
        del adset
    del camp
#     update_campaign_target(df_camp)
    check_campaign_metrics(campaign_id, campaign_metrics, table="campaign_leads_metrics")
    return


# In[9]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

# import fb_graph
# In[ ]:
DATABASE="dev_facebook_test"
HOST = "aws-dev-ai-private.adgeek.cc"
USER = "app"
PASSWORD = "adgeek1234"

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        passwd=PASSWORD,
        database=db_name
    )
    return mydb


# In[10]:


def insertion(table, df):
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()


# In[11]:


def update_campaign_target(df_camp):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = ("UPDATE campaign_target SET charge_type = %s, cost_per_target = %s, daily_budget = %s, daily_charge = %s, destination = %s, impressions = %s, period = %s, spend = %s, spend_cap = %s, start_time = %s , stop_time=%s, target=%s, target_left=%s, target_type=%s WHERE campaign_id = %s")
    val = ( 
        df_camp['charge_type'].iloc[0],
        df_camp['cost_per_target'].iloc[0].astype(dtype=object),
        df_camp['daily_budget'].iloc[0].astype(dtype=object),
        df_camp['daily_charge'].iloc[0].astype(dtype=object),
        df_camp['destination'].iloc[0].astype(dtype=object),
        df_camp['impressions'].iloc[0].astype(dtype=object),
        df_camp['period'].iloc[0].astype(dtype=object),
        df_camp['spend'].iloc[0].astype(dtype=object),
        df_camp['spend_cap'].iloc[0].astype(dtype=object),
        df_camp['start_time'].iloc[0],
        df_camp['stop_time'].iloc[0],
        df_camp['target'].iloc[0].astype(dtype=object),
        df_camp['target_left'].iloc[0].astype(dtype=object),
        df_camp['target_type'].iloc[0],
        df_camp['campaign_id'].iloc[0].astype(dtype=object)
    )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return


# In[12]:


def check_campaign_metrics(campaign_id, campaign_metrics, table=None):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM {} WHERE campaign_id={}".format(table, campaign_id), con=mydb )
    df_camp = pd.DataFrame(campaign_metrics, index=[0])
    df_camp['campaign_id'] = campaign_id
    if df.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df_camp.to_sql(table, conn, if_exists='append',index=False)
            engine.dispose()
        mydb.close()
        return False
    else:      
        try:
            mycursor = mydb.cursor()
            for col in campaign_metrics.keys():
                sql = "UPDATE {} SET {}={} where campaign_id={}".format(table, col, campaign_metrics[col], campaign_id)
                mycursor.execute(sql)
                mydb.commit()
            mycursor.close()
            mydb.close()
            return True
        except:
            pass
        
def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)

    df_check = pd.read_sql( "SELECT * FROM leadgen_optimal_weight WHERE campaign_id=%s" % (campaign_id), con=mydb )
    
    if df_check.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql("leadgen_optimal_weight", conn, if_exists='append',index=False)
            engine.dispose()
        mydb.close()
        return 
    else:
        mycursor = mydb.cursor()
        sql = "UPDATE leadgen_optimal_weight SET score=%s, w1=%s, w2=%s, w3=%s, w4=%s, w5=%s, w_spend=%s, w_bid=%s WHERE campaign_id=%s"
        val = (
            df['score'].iloc[0].astype(dtype=object),
            df['w1'].iloc[0].astype(dtype=object),
            df['w2'].iloc[0].astype(dtype=object),
            df['w3'].iloc[0].astype(dtype=object),
            df['w4'].iloc[0].astype(dtype=object),
            df['w5'].iloc[0].astype(dtype=object),
            df['w_spend'].iloc[0].astype(dtype=object),
            df['w_bid'].iloc[0].astype(dtype=object),
            df['campaign_id'].iloc[0].astype(dtype=object)
        )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return

# In[13]:


def get_campaign_target():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_list = df['campaign_id'].unique()
    df_camp = pd.DataFrame(columns=df.columns)
    for campaign_id in campaignid_list:
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        start_time = df['start_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time and start_time <= request_time:
            df_temp = df[df.campaign_id==campaign_id]
            df_camp = pd.concat([df_camp, df_temp])
    mydb.close()
    return df_camp
'''
for testing
        df_temp = df[df.campaign_id==campaign_id]
        df_camp = pd.concat([df_camp, df_temp])
# '''



# In[14]:


def main():
    start_time = datetime.datetime.now()
    
    df_camp = get_campaign_target()
    for campaign_id in df_camp.campaign_id.unique():
        destination = df_camp[df_camp.campaign_id==campaign_id].destination.iloc[0]
        charge_type = df_camp[df_camp.campaign_id==campaign_id].charge_type.iloc[0]
        print(campaign_id, df_camp[df_camp.campaign_id==campaign_id].charge_type.iloc[0])
        data_collect( campaign_id, destination, charge_type )#存資料
    print(datetime.datetime.now()-start_time)
    import gc
    gc.collect()


# In[2]:


if __name__=='__main__':
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    main()
#     data_collect(23843003561380761, 70, "LEAD_GENERATION")


# In[5]:


#get_ipython().system('jupyter nbconvert --to script facebook_leadgen_index_collector.ipynb')


# In[ ]:




