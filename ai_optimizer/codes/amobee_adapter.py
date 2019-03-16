import requests
import json
import datetime
import pandas as pd
import amobee_db
import bid_operator

DATADASE = "Amobee"
START_TIME = 'start_time'
STOP_TIME = 'stop_time'

CHARGE = 'charge'
TARGET = 'target'
BID_AMOUNT = 'bid_amount'
REQUEST_TIME = 'request_time'
TARGET_LEFT = 'target_left'

INIT_BID = 'init_bid'
LAST_BID = 'last_bid'
PACKAGE_PROGRESS = 'package_progress'
IO_PROGRESS = 'io_progress'

PACKAGE_ID = 'package_id'
IOID = 'ioid'

class AmobeeInsertionOrderAdapter(object):
    def __init__(self, ioid):
        self.mydb = amobee_db.connectDB( DATADASE )
        self.limit = 9000
        self.hour_per_day = 24
        self.ioid = ioid
        self.request_time = datetime.datetime.now()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
    def get_df(self):
        self.df_io = pd.read_sql( "SELECT * FROM io_target WHERE ioid=%s" %( self.ioid ), con=self.mydb )
        self.df_pkg = pd.read_sql( "SELECT * FROM package_insights where ioid = %s" %( self.ioid ), con=self.mydb )
        return
    
    def get_bid(self):
        sql = "SELECT package_id, bid_amount, request_time FROM package_insights WHERE ioid=%s ;" % ( self.ioid )
        df_pkg = pd.read_sql( sql, con=self.mydb )
        package_list = df_pkg['package_id'].unique()
        for package in package_list:
            init_bid = df_pkg[BID_AMOUNT][df_pkg.package_id==package].head(1).iloc[0].astype(dtype=object)
            last_bid = df_pkg[BID_AMOUNT][df_pkg.package_id==package].tail(1).iloc[0].astype(dtype=object)
            self.init_bid_dict.update({ package: init_bid })
            self.last_bid_dict.update({ package: last_bid })
        return
    
    def get_period_left(self):
        self.period_left = ( self.df_io[ STOP_TIME ].iloc[0] - self.request_time ).days
        return self.period_left
    
    def get_period(self):
        self.period = ( self.df_io[ STOP_TIME ].iloc[0] - self.df_io[ START_TIME ].iloc[0] ).days
        return self.period
    
    def get_io_performance(self):
        package_list = self.df_pkg[ 'package_id' ].unique()
        dfs = pd.DataFrame(columns=[ 'package_id', TARGET ])
        for package in package_list:
            df_pkg = self.df_pkg[self.df_pkg.package_id==package]
            df = df_pkg[[ TARGET, REQUEST_TIME ]][df_pkg.request_time.dt.date==self.request_time]
            dfs = pd.concat([dfs, df], axis=0, sort=False)
        dfs = dfs.sort_values(by=[ TARGET ], ascending=False).reset_index(drop=True)
        self.io_performance = dfs[ TARGET ].sum()
        return self.io_performance
    
    def get_io_target(self):
        self.io_target = self.df_io[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        return self.io_target
    
    def get_io_day_target(self):
        self.io_day_target = self.io_target / self.period_left
        return self.io_day_target

    def get_io_progress(self):
        self.io_progress = self.io_performance / self.io_day_target
        return self.io_progress
    
    def get_package_list(self):
        return self.df_pkg[ 'package_id' ].unique().tolist()
    
    def retrieve_io_attribute(self):
        self.get_df()
        self.get_bid()
        self.get_period_left()
        self.get_period()
        self.get_io_performance()
        self.get_io_target()
        self.get_io_day_target()
        self.get_io_progress()
        return
    
class AmobeePackageAdapter(AmobeeInsertionOrderAdapter):
    def __init__(self, package_id, amobee):
        self.mydb = amobee_db.connectDB( DATADASE )
        self.package_id = package_id
        self.amobee = amobee

    def init_io(self, amobee):
        self.time_progress = amobee.time_progress
        self.limit = amobee.limit
        self.hour_per_day = amobee.hour_per_day
        self.request_time = amobee.request_time
        self.df_pkg = amobee.df_pkg
        self.init_bid_dict = amobee.init_bid_dict
        self.last_bid_dict = amobee.last_bid_dict
        self.period_left = amobee.period_left
        self.period = amobee.period
        self.io_performance = amobee.io_performance
        self.io_target = amobee.io_target
        self.io_day_target = amobee.io_day_target
        self.io_progress = amobee.io_progress
        return
    
    def get_ioid(self):
        self.ioid = self.df_pkg[ 'ioid' ].iloc[0].astype(dtype=object)
        return self.ioid
    
    def get_package_day_target(self):
        pkg_num = len( self.df_pkg[ 'package_id' ].unique() )
        self.package_day_target = self.io_day_target / pkg_num
        return self.package_day_target
    
    def get_package_performance(self):
        self.package_performance = self.df_pkg[ TARGET ][self.df_pkg.package_id==self.package_id].iloc[0]
        return self.package_performance
    
    def get_bid(self):
        self.init_bid = self.init_bid_dict[self.package_id]
        self.last_bid = self.last_bid_dict[self.package_id]
        return
    
    def get_package_time_target(self):
        self.package_time_target = self.package_day_target * self.time_progress
        return self.package_time_target
    
    def get_package_progress(self):
        self.package_progress = self.package_performance / self.package_time_target
        return self.package_progress
    
    def retrieve_package_attribute(self):
        self.init_io(self.amobee)
        self.get_ioid()
        self.get_package_day_target()
        self.get_package_performance()
        self.get_bid()
        self.get_package_time_target()
        self.get_package_progress()
        return {
            PACKAGE_ID:self.package_id,
            INIT_BID:self.init_bid,
            LAST_BID:self.last_bid,
            PACKAGE_PROGRESS:self.package_progress,
            IO_PROGRESS:self.io_progress
        }
    
def main():
    start_time = datetime.datetime.now()
    
    df_io = amobee_db.get_io_target()
    ioid_list = df_io['ioid'].unique()
    
    for ioid in ioid_list:
        result={ 'media': 'Amobee', 'ioid': ioid, 'contents':[] }
        try:
            amobee = AmobeeInsertionOrderAdapter( ioid=ioid )
            amobee.retrieve_io_attribute()
            package_list = amobee.get_package_list()
            for package in package_list:
                pkg = AmobeePackageAdapter( package, amobee )
                status = pkg.retrieve_package_attribute()
                media = result['media']
                bid = bid_operator.adjust(media, **status)
                result['contents'].append(bid)
                del pkg
            mydict_json = json.dumps(result)
            amobee_db.insert_result( ioid, mydict_json, datetime.datetime.now() )
            del amobee
        except:
            print('pass')
            pass
    print(datetime.datetime.now()-start_time)
    return
    
if __name__=='__main__':
    main()