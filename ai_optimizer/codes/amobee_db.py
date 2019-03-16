import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

DATABASE="Amobee"

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
        if table == "io_target":
            df.to_sql("io_target", conn, if_exists='append',index=False)
        if table == "package_insights":
            df.to_sql("package_insights", conn, if_exists='append',index=False)

def check_io_target(ioid, destination, charge_type):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM io_target WHERE ioid=%s" % (ioid), con=mydb )   
    if df.empty:
        mycursor = mydb.cursor()
        sql = "INSERT INTO io_target ( ioid, destination, target_type ) VALUES ( %s, %s, %s )"
        val = ( ioid, destination, charge_type )
        mycursor.execute(sql, val)
        mydb.commit()
        return False
    else:
        sql = "UPDATE io_target SET destination=%s, target_type=%s WHERE ioid=%s"
        val = ( destination, charge_type, ioid )
        mycursor = mydb.cursor()
        mycursor.execute(sql, val)
        mydb.commit()
        return True

def update_io_target(df_io):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = ("update io_target set actions=%s, cost_per_impresions=%s, cost_per_target=%s, daily_budget=%s, daily_charge=%s, destination=%s, goal_cpc=%s, impressions=%s, period=%s, spend=%s, spend_cap=%s, start_time=%s, stop_time=%s, target=%s, target_left=%s, target_type=%s where ioid=%s")
    val = (
            df_io['actions'].iloc[0].astype(dtype=object),
            df_io['cost_per_impresions'].iloc[0].astype(dtype=object),
            df_io['cost_per_target'].iloc[0].astype(dtype=object),
            df_io['daily_budget'].iloc[0].astype(dtype=object),
            df_io['daily_charge'].iloc[0].astype(dtype=object),
            df_io['destination'].iloc[0].astype(dtype=object),
            df_io['goal_cpc'].iloc[0].astype(dtype=object),
            df_io['impressions'].iloc[0].astype(dtype=object),
            df_io['period'].iloc[0].astype(dtype=object),
            df_io['spend'].iloc[0].astype(dtype=object),
            df_io['spend_cap'].iloc[0].astype(dtype=object),
            df_io['start_time'].iloc[0].isoformat(),
            df_io['stop_time'].iloc[0].isoformat(),
            df_io['target'].iloc[0].astype(dtype=object),
            df_io['target_left'].iloc[0].astype(dtype=object),
            df_io['target_type'].iloc[0],
            df_io['ioid'].iloc[0].astype(dtype=object)
        )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_io_target():
    request_time = datetime.datetime.now()
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    df = pd.read_sql( "SELECT * FROM io_target" , con=mydb )
    ioid_list = df['ioid'].unique()
    df_io = pd.DataFrame(columns=df.columns)
    for ioid in ioid_list:
        start_time = df['start_time'][df.ioid==ioid].iloc[0]
        stop_time = df['stop_time'][df.ioid==ioid].iloc[0]
        if stop_time >= request_time and start_time <= request_time:
            df_io = pd.concat([df_io, df[df.ioid==ioid]])
    return df_io

def check_default_price(ioid):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM default_price WHERE ioid={}".format(ioid), con=mydb )
    if df.empty:
        return True
    else:
        return False

def insert_default( ioid, mydict ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO default_price ( ioid, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( ioid, mydict, datetime.datetime.now() )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_default( ioid ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM default_price WHERE ioid=%s ORDER BY request_time DESC LIMIT 1" % (ioid) )
    default = mycursor.fetchone()
    return default[0]

def insert_result( ioid, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( ioid, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( ioid, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    return

'''
    sql = ("update io_target set actions={0}, cost_per_impresions={1}, cost_per_target={2}, daily_budget={3}, daily_charge={4}, destination={5}, goal_cpc={6}, impressions={7}, period={8}, spend={9}, spend_cap={10}, start_time={11}, stop_time={12}, target={13}, target_left={14}, target_type={15} where ioid={16}".format(
            df_io['actions'].iloc[0].astype(dtype=object),
            df_io['cost_per_impresions'].iloc[0].astype(dtype=object),
            df_io['cost_per_target'].iloc[0].astype(dtype=object),
            df_io['daily_budget'].iloc[0].astype(dtype=object),
            df_io['daily_charge'].iloc[0].astype(dtype=object),
            df_io['destination'].iloc[0].astype(dtype=object),
            df_io['goal_cpc'].iloc[0].astype(dtype=object),
            df_io['impressions'].iloc[0].astype(dtype=object),
            df_io['period'].iloc[0].astype(dtype=object),
            df_io['spend'].iloc[0].astype(dtype=object),
            df_io['spend_cap'].iloc[0].astype(dtype=object),
            df_io['start_time'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'),
            df_io['stop_time'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'),
            df_io['target'].iloc[0].astype(dtype=object),
            df_io['target_left'].iloc[0].astype(dtype=object),
            df_io['target_type'].iloc[0],
            df_io['ioid'].iloc[0].astype(dtype=object)
        )
              )
'''