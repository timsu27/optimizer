import requests
import json
import datetime
import pandas as pd
import amobee_db
class DatePreset:
    today = 'today'
    lifetime = 'lifetime'

class AmobeeField:
    clientStatusId = 'clientStatusId'
    creativeRotationType = 'creativeRotationType'
    goalValue = 'goalValue'
    pastBudgetSchedules = 'pastBudgetSchedules'
    spendingCap = 'spendingCap'
    startDate = 'startDate'
    endDate = 'endDate'
    data = 'data'
    items = 'items'
    packageId = 'packageId'
    advertiser_invoice = 'advertiser_invoice'
    clicks = 'clicks'
    advertiser_ecpm = 'advertiser_ecpm'
    dcpmBid = 'dcpmBid'
    cpmBid = 'cpmBid'
    bidding = 'bidding'
    budgeting = 'budgeting'
    budgetSchedule = 'budgetSchedule'
    cpc = 'cpc'
    maxAvgBid = 'maxAvgBid'
    
class AdgeekField:
    package_id = 'package_id'
    spend_cap = 'spend_cap'
    start_time = 'start_time'
    stop_time = 'stop_time'
    target_type = 'target_type'
    goal_cpc = 'goal_cpc'
    period = 'period'
    ioid = 'ioid'
    cost_per_impresions = 'cost_per_impresions'
    cost_per_target = 'cost_per_target'
    daily_budget = 'daily_budget'
    spend = 'spend'
    target = 'target'
    bid_amount = 'bid_amount'
    request_time = 'request_time'
    daily_charge = 'daily_charge'
    target_left = 'target_left'
    destination = 'destination'

class InsertionOrders(object):
    def __init__(self, ioid):
        self.ioid = ioid
        self.start_date = None
        self.end_date = None
        self.spend_cap = None
        self.package_list = list()
        self.io_insights = dict()
        self.io_features = dict()
        
    def get_insertion_order_features(self):
        url = "https://services.amobee.com/metadata/v2/api/insertionorders/{}".format(self.ioid)
        headers = {"Authorization":"Bearer {}".format(access_token)}
        r = requests.get(url, headers=headers)
        metadata = json.loads(r.text)
        status = metadata[ AmobeeField.clientStatusId ]
        target_type = metadata[ AmobeeField.creativeRotationType ]
        goal_cpc = metadata[ AmobeeField.goalValue ]
        schedule = metadata[ AmobeeField.pastBudgetSchedules ]
        for sch in schedule:
            self.spend_cap = sch[ AmobeeField.spendingCap ]
            self.start_date = datetime.datetime.strptime( sch[ AmobeeField.startDate ],'%Y-%m-%d' )
            self.end_date = datetime.datetime.strptime( sch[ AmobeeField.endDate ],'%Y-%m-%d' )
        self.io_features.update( {AdgeekField.spend_cap: self.spend_cap} )
        self.io_features.update( {AdgeekField.start_time: self.start_date} )
        self.io_features.update( {AdgeekField.stop_time: self.end_date} )
        self.io_features.update( {AdgeekField.target_type: target_type} )
        self.io_features.update( {AdgeekField.goal_cpc: goal_cpc} )
        self.period = (self.io_features[AdgeekField.stop_time] - self.io_features[AdgeekField.start_time]).days
        self.io_features.update( {AdgeekField.period: self.period } )
        
        return self.io_features
    
    def get_insertion_order_insights(self, date_preset=None):
        if date_preset is None or date_preset==DatePreset.lifetime:
            start_time = self.start_date.strftime( '%Y-%m-%d' )
            stop_time = ( datetime.datetime.now() - datetime.timedelta(1) ).strftime( '%Y-%m-%d' )
        elif date_preset==DatePreset.today:
            start_time = ( datetime.datetime.now() - datetime.timedelta(1) ).strftime( '%Y-%m-%d' )
            stop_time = ( datetime.datetime.now() - datetime.timedelta(1) ).strftime( '%Y-%m-%d' )
        url = "https://services.amobee.com/reporting/v1/api/insertionorders/{}?reportType=summary&startDate={}&endDate={}".format(self.ioid, start_time, stop_time)
        headers = {"Authorization":"Bearer {}".format(access_token)}
        r = requests.get(url, headers=headers)
        insights = json.loads(r.text)[ AmobeeField.data ]
        for d in insights:
            self.io_insights.update( { d['description'] : float(d['value']) } )
#         self.io_insights.update( {v: float(k) for k, v in tmp.items()} )
        return self.io_insights
    
    def get_package_list(self):
        url = "https://services.amobee.com/metadata/v2/api/packages/ioid/{}".format(self.ioid)
        headers = {"Authorization":"Bearer {}".format(access_token)}
        r = requests.get(url, headers=headers)
        for package in json.loads(r.text)[ AmobeeField.items ]:
            self.package_list.append( package[ AmobeeField.packageId ] )
        return self.package_list
    
    def initialize(self):
        self.get_insertion_order_features()
        self.get_insertion_order_insights( date_preset=DatePreset.lifetime )
        self.io_insights.update( {AdgeekField.ioid:self.ioid} )
        self.io_insights[AdgeekField.spend] = self.io_insights.pop( AmobeeField.advertiser_invoice)
        self.io_insights[AdgeekField.target] = self.io_insights.pop( AmobeeField.clicks )
        self.io_insights[AdgeekField.cost_per_impresions] = self.io_insights.pop( AmobeeField.advertiser_ecpm )
        self.insertion_order = { **self.io_insights, **self.io_features }
        self.insertion_order.update( {AdgeekField.cost_per_target: self.io_insights[AdgeekField.spend] / self.io_insights[AdgeekField.target] } )
        self.insertion_order.update( {AdgeekField.daily_budget:self.io_features[AdgeekField.spend_cap] / self.io_features[AdgeekField.period] } )
        return self.insertion_order
    
class Package(object):
    def __init__(self, package_id):
        self.package_id = int(package_id)
        self.package_features = dict()
        self.package_insights = dict()
        
    def get_package_features(self):
        url = "https://services.amobee.com/metadata/v2/api/packages/{}".format(self.package_id)
        headers = {"Authorization":"Bearer {}".format(access_token)}
        r = requests.get(url, headers=headers)
        metadata = json.loads(r.text)
        bidding = metadata[ AmobeeField.bidding ]
        try:
            self.bid = bidding[ AmobeeField.dcpmBid ][ AmobeeField.maxAvgBid ]
        except:
            self.bid = bidding[ AmobeeField.cpmBid ]
        budgeting = metadata[ AmobeeField.budgeting ]
        schedule = budgeting[ AmobeeField.budgetSchedule ]
        for sch in schedule:
            self.daily_budget = sch[ AmobeeField.spendingCap ]
        self.package_features.update( {AdgeekField.daily_budget: self.daily_budget} )
        self.package_features.update( {AdgeekField.bid_amount: self.bid} )
        self.package_features.update( {AdgeekField.package_id: self.package_id} )
        return self.package_features
    
    def get_package_insights(self, date_preset=None):
        if date_preset is None or date_preset==DatePreset.today:
            start_time = ( datetime.datetime.now() - datetime.timedelta(1) ).strftime( '%Y-%m-%d' )
        stop_time = ( datetime.datetime.now() - datetime.timedelta(1) ).strftime( '%Y-%m-%d' )
        url = "https://services.amobee.com/reporting/v1/api/packages/{}?reportType=summary&startDate={}&endDate={}".format(self.package_id, start_time, stop_time)
        headers = {"Authorization":"Bearer {}".format(access_token)}
        r = requests.get(url, headers=headers)
        insights = json.loads(r.text)[ AmobeeField.data ]
        for d in json.loads(r.text)[ AmobeeField.data ]:
            self.package_insights.update( { d['description'] : float(d['value']) } )
        return self.package_insights
    
    def initialize(self):
        self.get_package_features()
        self.get_package_insights()
        self.package_insights.update( {AdgeekField.package_id:self.package_id} )
        self.package_insights[ AdgeekField.spend ] = self.package_insights.pop( AmobeeField.advertiser_invoice)
        self.package_insights[ AdgeekField.target ] = self.package_insights.pop( AmobeeField.clicks )
        self.package_insights[ AdgeekField.cost_per_impresions ] = self.package_insights.pop( AmobeeField.advertiser_ecpm )
        try:
            self.package_insights.update( {AdgeekField.cost_per_target: self.package_insights[AdgeekField.spend] / self.package_insights[AdgeekField.target] } )
        except ZeroDivisionError:
            self.package_insights.update( {AdgeekField.cost_per_target: 0} )
        self.package = { **self.package_insights, **self.package_features }
        return self.package

def get_access_token():
    url = "https://services.amobee.com/accounts/v1/api/token"
    headers = {"Content-Type":"application/json"}
    payload = {
    "client_id":"ad2819a6-5f55-1038-834c-5bbb75af789b@658.api",
    "client_secret":"Lmrp3yqrzToo3NM/",
    "grant_type":"client_credentials"
    }
    data = json.dumps(payload)
    r = requests.post(url, headers=headers, data=data)
    global access_token
    access_token = json.loads(r.text)['access_token']
    return access_token

def data_collect( ioid, total_clicks ):
    io = InsertionOrders( ioid=ioid )
    io.initialize()
    charge = io.insertion_order[ AdgeekField.target ]
    target_left = { AdgeekField.target_left: int(total_clicks) - int(charge) }
    daily_charge = { AdgeekField.daily_charge: int(total_clicks) / int(io.period) }
    request_dict = { AdgeekField.request_time: datetime.datetime.now() }
    target_dict = { AdgeekField.destination: total_clicks }
    io_dict = {
        **io.insertion_order,
        **target_left,
        **target_dict,
        **daily_charge,
    }
    package_list = io.get_package_list()
    for package in package_list:
        pkg = Package(package_id=package)
        pkg.initialize()
        pkg.package.update( { AdgeekField.request_time: datetime.datetime.now() } )
        pkg.package.update( { AdgeekField.ioid: io.ioid } )
        df_pkg = pd.DataFrame(pkg.package, index=[0])
        amobee_db.insertion( table="package_insights", df=df_pkg )
        
    df_io = pd.DataFrame(io_dict, index=[0])
    amobee_db.update_io_target(df_io)
    return df_io

def make_default( ioid, media, charge_type ):
    get_access_token()
    io = InsertionOrders( ioid=ioid )
    package_list = io.get_package_list()
    mydict=dict()
    result={ 'media': media, 'ioid': ioid, 'contents':[] }
    for package in package_list:
        pkg = Package( package_id=package )
        pkg_info = pkg.initialize()
        if not bool(pkg_info):
            pass
        else:
            result['contents'].append(
                {
                    'pred_cpc': str( pkg_info['bid_amount'] ),
                    'pred_budget': str( pkg_info['daily_budget'] ),
                    'adset_id': str( package ),
                }
            )
    if result['contents']:
        mydict_json = json.dumps(result)
        amobee_db.insert_default( str( ioid ), mydict_json )
    return

def main():
    start_time = datetime.datetime.now()
    get_access_token()
    df_io = amobee_db.get_io_target()
    ioid_list = df_io['ioid'].unique()
    for ioid in ioid_list:
        destination = df_io['destination'][df_io.ioid==ioid].iloc[0]
        print(ioid, df_io['target_type'][df_io.ioid==ioid].iloc[0])
        data_collect( ioid, destination )#存資料
    print(datetime.datetime.now()-start_time)
if __name__=="__main__":
    
    main()