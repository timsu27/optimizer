from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import sys
sys.path.append('ai_optimizer/codes/')
import json
import numpy as np
import pandas as pd
from sklearn.externals import joblib

from ai_optimizer.codes import mysql_adactivity_save
from ai_optimizer.codes import amobee_db
from ai_optimizer.codes import amobee_datacollector
from facebook_business.api import FacebookAdsApi
from facebook_datacollector import Campaigns
import facebook_datacollector
import datetime
# FOLDER_PATH = 'ai_optimizer/models/cpc_120/'
# MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'
class Field(object):
    pred_cpc = 'pred_cpc'
    pred_budget = 'pred_budget'
    target_TYPE = 'target_type'
    reasons = 'reasons'

    ad_id = 'ad_id'
    campaign_id = 'campaign_id'
    target = 'total_clicks'
    charge_type = 'charge_type'
    media = 'media'



my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

@csrf_exempt
def opt_api(request):
    if request.method == "POST":
        start_time = datetime.datetime.now()
        campaign_id = request.POST.get(Field.campaign_id)
        destination = request.POST.get(Field.target)
        charge_type = request.POST.get(Field.charge_type)
        media = request.POST.get(Field.media)
        print(campaign_id, destination, charge_type, media)
        print(media == None)
#         if campaign_id and destination and charge_type and media: # new release version
        if campaign_id and destination and charge_type: #temporary working version
            if media == 'Facebook' or media is None:
                FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
                queue = mysql_adactivity_save.check_campaignid_target( campaign_id, destination, charge_type )
                if mysql_adactivity_save.check_default_price(campaign_id):
                    facebook_datacollector.make_default( int(campaign_id), charge_type )
                if queue:
                    print(campaign_id, destination)
                    campaign = Campaigns( int(campaign_id), charge_type )
                    campaign_dict = campaign.to_campaign()
                    try:lifetime_target = campaign_dict['target']
                    except:lifetime_target=0
                    try:
                        target_left_dict = {
                            'target_left': int(destination) - int(lifetime_target)
                        }
                    except:
                        temp = mysql_adactivity_save.get_campaign_target_dict()
                        destination = temp[ int(campaign_id) ]
                        target_left_dict = {
                            'target_left': int(destination) - int(charge)
                        }
                    charge_dict = { 'charge_type': charge_type }
                    target_dict = { 'destination': int(destination) }
                    campaign_dict = {
                        **campaign_dict,
                        **charge_dict,
                        **target_dict,
                        **target_left_dict,
                    }
                    df_camp = pd.DataFrame( campaign_dict, index=[0] )
                    mysql_adactivity_save.update_campaign_target(df_camp)
                    try:
#                         mydict = mysql_adactivity_save.get_result( campaign_id ) #new version
                        mydict = mysql_adactivity_save.get_release_result( campaign_id ) #release version
                    except:
#                         mydict = mysql_adactivity_save.get_default( campaign_id ) #new version
                        mydict = mysql_adactivity_save.get_release_default( campaign_id )#release version
                else:
                    mydict = mysql_adactivity_save.get_default( campaign_id ) #new version
                    mydict = mysql_adactivity_save.get_release_default( campaign_id )#release version
                return JsonResponse( json.loads(mydict), safe=False )
            elif media == 'Amobee':
                queue = amobee_db.check_io_target( int(campaign_id), destination, charge_type )
                if amobee_db.check_default_price(campaign_id):
                    amobee_datacollector.make_default( int(campaign_id), media, charge_type )
                mydict = amobee_db.get_default( campaign_id )
                return JsonResponse( json.loads(mydict), safe=False )
        elif campaign_id:
            try:
#                         mydict = mysql_adactivity_save.get_result( campaign_id ) #new version
                mydict = mysql_adactivity_save.get_release_result( campaign_id ) #release version
            except:
#                         mydict = mysql_adactivity_save.get_default( campaign_id ) #new version
                mydict = mysql_adactivity_save.get_release_default( campaign_id )#release version
            return JsonResponse( json.loads(mydict), safe=False )
    else:
        return JsonResponse( {}, safe=False )
        