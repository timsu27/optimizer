#!/usr/bin/env python
# coding: utf-8

# In[ ]:

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

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="aws-dev-ai-private.adgeek.cc",
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb


# In[ ]:

    
#status

def insertSelection(campaign_id, adset_id, ad_id, selection):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO status ( campaign_id, adset_id, ad_id, status ) VALUES ( %s, %s, %s, %s )"
    val = ( campaign_id, adset_id, ad_id, selection )
    mycursor.execute(sql, val)
    mydb.commit()
    return

#default_price

def insert_default( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO default_price ( campaign_id, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_default( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    default = str(default[0][0], encoding='utf-8')
    mycursor.close()
    mydb.close()
    return default

def check_default_price(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM default_price WHERE campaign_id=%s" % (campaign_id), con=mydb )
    mydb.close()
    if df.empty:
        return True
    else:
        return False
#result

def insert_result( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_result( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    results = str(results[0][0], encoding='utf-8')
    mycursor.close()
    mydb.close()
    return results

#campaign_target

def check_campaignid_target(campaign_id, destination, charge_type):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )   
    if df.empty:
        mycursor = mydb.cursor()
        sql = "INSERT INTO campaign_target ( campaign_id, destination, charge_type ) VALUES ( %s, %s, %s )"
        val = ( campaign_id, destination, charge_type )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return False
    else:
        sql = "UPDATE campaign_target SET destination=%s, charge_type=%s WHERE campaign_id=%s"
        val = ( destination, charge_type, campaign_id )
        mycursor = mydb.cursor()
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return True

def get_campaign_target_dict():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
        campaignid_dict[campaign_id]=df['destination'][df.campaign_id==campaign_id]
#         stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
#         if stop_time >= request_time:
#             campaignid_dict[campaign_id]=df['destination'][df.campaign_id==campaign_id]
    mydb.close()
    return campaignid_dict

def get_campaign_target_left_dict():
    DATABASE = "ad_activity"
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
#         print(df[df.campaign_id==campaign_id])
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time:
            campaignid_dict[campaign_id]=df['target_left'][df.campaign_id==campaign_id]
    mydb.close()
    return campaignid_dict

def get_campaign():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaign_id_list = df['campaign_id'].unique()
    mydb.close()
    return campaign_id_list
### optimal_weight ###

def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM optimal_weight WHERE campaign_id={}".foramt(campaign_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/dev_facebook_test' )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "optimal_weight", conn, if_exists='append',index=False )
        engine.dispose()
        return
    else:
        mycursor = mydb.cursor()
        sql = "UPDATE optimal_weight SET score=%s, weight_kpi=%s, weight_spend=%s, weight_bid=%s WHERE campaign_id=%s"
        val = ( df['score'].iloc[0].astype(dtype=object),
                df['weight_kpi'].iloc[0].astype(dtype=object),
                df['weight_spend'].iloc[0].astype(dtype=object),
                df['weight_bid'].iloc[0].astype(dtype=object),
                df['campaign_id'].iloc[0].astype(dtype=object)
              )
#         sql = "UPDATE optimal_weight SET score=%s, weight_kpi=%s, weight_spend=%s, weight_bid=%s, weight_width=%s WHERE campaign_id=%s"
#         val = ( df['score'].iloc[0].astype(dtype=object),
#                 df['weight_kpi'].iloc[0].astype(dtype=object),
#                 df['weight_spend'].iloc[0].astype(dtype=object),
#                 df['weight_bid'].iloc[0].astype(dtype=object),
#                 df['weight_width'].iloc[0].astype(dtype=object),
#                 df['campaign_id'].iloc[0].astype(dtype=object)
#               )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return
######## NEW ######

def get_campaign_target(campaign_id):
    mydb = connectDB(DATABASE)
    df_camp = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )
    mydb.close()
    return df_camp

def update_campaign_target(df_camp):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    try:
        sql = ("UPDATE campaign_target SET charge_type = %s, cost_per_target = %s, daily_budget = %s, daily_charge = %s, destination = %s, impressions = %s, period = %s, reach = %s, spend = %s, spend_cap = %s, start_time = %s , stop_time=%s, target=%s, target_left=%s, target_type=%s WHERE campaign_id = %s")
        val = ( 
            df_camp['charge_type'].iloc[0],
            df_camp['cost_per_target'].iloc[0].astype(dtype=object),
            df_camp['daily_budget'].iloc[0].astype(dtype=object),
            df_camp['daily_charge'].iloc[0].astype(dtype=object),
            df_camp['destination'].iloc[0].astype(dtype=object),
            df_camp['impressions'].iloc[0].astype(dtype=object),
            df_camp['period'].iloc[0].astype(dtype=object),
            df_camp['reach'].iloc[0].astype(dtype=object),
            df_camp['spend'].iloc[0].astype(dtype=object),
            df_camp['spend_cap'].iloc[0].astype(dtype=object),
            df_camp['start_time'].iloc[0],
            df_camp['stop_time'].iloc[0],
            df_camp['target'].iloc[0].astype(dtype=object),
            df_camp['target_left'].iloc[0].astype(dtype=object),
            df_camp['target_type'].iloc[0],
            df_camp['campaign_id'].iloc[0].astype(dtype=object)
        )
    except:
        sql = "UPDATE campaign_target SET target_type = %s,spend_cap = %s, start_time = %s, stop_time = %s , period=%s, daily_budget=%s WHERE campaign_id = %s"
        val = ( 
            df_camp['target_type'].iloc[0], 
            df_camp['spend_cap'].iloc[0], 
            df_camp['start_time'].iloc[0], 
            df_camp['stop_time'].iloc[0], 
            df_camp['period'].iloc[0].astype(dtype=object), 
            df_camp['daily_budget'].iloc[0].astype(dtype=object), 
            df_camp['campaign_id'].iloc[0]
        )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def intoDB(table, df):
    engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/{}'.format(DATABASE) )
#     print(df.columns)
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()
            
def insert_release_result( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO release_ver_result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_release_result( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM release_ver_result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    try:
        results = str(results[0][0], encoding='utf-8')
        mycursor.close()
        mydb.close()
        return results
    except:
        return str(dict())

def insert_release_default( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO release_default_price ( campaign_id, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_release_default( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM release_default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    try:
        default = str(default[0][0], encoding='utf-8')
        mycursor.close()
        mydb.close()
        return default
    except:
        return str(dict())

def check_release_default_price(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM release_default_price WHERE campaign_id=%s" % (campaign_id), con=mydb )
    mydb.close()
    if df.empty:
        return True
    else:
        return False

def update_init_bid(adset_id, init_bid):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "UPDATE adset_insights SET bid_amount={} WHERE adset_id={} LIMIT 1".format(init_bid, adset_id)
    mycursor.execute(sql)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def check_initial_bid(adset_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM adset_initial_bid WHERE adset_id={}".format(adset_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/dev_facebook_test' )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "adset_initial_bid", conn, if_exists='append',index=False )
        engine.dispose()
    return
    
def update_init_bid_by_campaign(campaign_id):
    df_camp = get_campaign_target(campaign_id)
    init_bid = input("type init_bid u want to change:")
    adset_list = Campaigns(campaign_id, df_camp['charge_type'].iloc[0]).get_adsets()
    for adset_id in adset_list:
        update_init_bid( int(adset_id), init_bid )
    mydb.close()
    return

if __name__=="__main__":
#     get_campaign_target()
    from facebook_datacollector import Campaigns
    campaign_id = input("input campaign_id u want to change:")
    update_init_bid_by_campaign(campaign_id)
    
    
    