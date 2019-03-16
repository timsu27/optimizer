import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

DATABASE="Brightroll"

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="localhost",
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb

def insertion(table, df):
    engine = create_engine( 'mysql://app:adgeek1234@localhost/{}'.format(DATABASE) )
    with engine.connect() as conn, conn.begin():
        if table == "campaign_target":
            df.to_sql("line_target", conn, if_exists='append',index=False)
        if table == "line_insights":
            df.to_sql("line_insights", conn, if_exists='append',index=False)
            
def get_campaign_target():
    request_time = datetime.datetime.now()
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaign_list = df['campaign_id'].unique()
    df_camp = pd.DataFrame(columns=df.columns)
    for campaign_id in campaign_list:
        start_time = df['start_time'][df.campaign_id==campaign_id].iloc[0]
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time and start_time <= request_time:
            df_camp = pd.concat([df_camp, df[df.campaign_id==campaign_id]])
    return df_camp
            
def get_line_insights(campaign_id):
    request_time = datetime.datetime.now()
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    df = pd.read_sql( "SELECT * FROM line_insights WHERE campaign_id={}".format(campaign_id) , con=mydb )
    line_list = df['line_id'].unique()
    df_line = pd.DataFrame(columns=df.columns)
#     for campaign_id in campaign_list:
#         start_time = df['start_time'][df.campaign_id==campaign_id].iloc[0]
#         stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
#         if stop_time >= request_time and start_time <= request_time:
#             df_camp = pd.concat([df_camp, df[df.campaign_id==campaign_id]])
    return df_line