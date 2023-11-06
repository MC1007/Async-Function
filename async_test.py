import requests
import pandas as pd
import json
import datetime
from pytz import timezone
import numpy as np
import sys
import urllib3
import asyncio
import aiohttp
import time

from request_boost_test import get_access_token
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

get_access_token()
start_time = time.time()

sitenames_with_spaces = {
    'rojmal_inox': 'rojmal_inox ',
    'pratapgarh_gamesa': 'pratapgarh_gamesa '
}

signal_id_url = "https://api-srvs.rendigital.apps.ge.com/machinedata/v1/realtime-signals"
signal_url = "https://api-srvs.rendigital.apps.ge.com/machinedata/v1/historical-signals"

tag_list = pd.read_excel(r"C:\Users\bhava\Desktop\Tata Wind\Tata Wind\Canonical_signal_ids.xlsx")
site = tag_list.columns.tolist()
print(site)
df_data = pd.DataFrame()
mylist = []
data_list = []
ids = []

if len(sys.argv) < 2:
    print("please provide an argument.")
    sys.exit(1)

input_para = str(sys.argv[1])

if input_para in site:
    print(f'{input_para} is present in the list')
    mylist = tag_list[input_para].dropna().tolist()
else:
    print('check in dictionary')
    if input_para in sitenames_with_spaces.keys():
        print("present in the dictionary")
        sitenames_changed = sitenames_with_spaces[input_para]
        input_para = sitenames_changed
        mylist = tag_list[input_para].dropna().tolist()
    else:
        raise Exception(f'{input_para} is not present in the dictionary')

def get_header(tenant_name, tenant_id): 
    with open("access_token2.txt", "r") as txt_file:
        access_token_2 = txt_file.read().strip()

    headers = {
        "Authorization": f"Bearer {access_token_2}",
        tenant_name: tenant_id
    }
    return headers

def signal_id_response(input_para):
    headers = get_header("tatatenant", "tata00NVky")
    params = {
        'sourceIds': input_para,
        'signalIds': mylist
    }

    response = requests.get(signal_id_url, headers=headers, params=params, verify=False)
    print(response)
    json_data = response.json()
    signal_data = json_data['signalData']

    df = pd.DataFrame(signal_data)
    df = df[signal_data[0].keys()]

    unique_signalIds = df['signalId'].unique()
    assetsId_list = df['assetId'].unique()

    not_responding = []

    for element in mylist:
        if element not in unique_signalIds:
            not_responding.append(element)
    
    return assetsId_list, unique_signalIds



async def send_request(session, url, headers, params):
    async with session.get(url, params=params, headers=headers) as response:
        # print(response)
        return await response.json()
        # return response
    # return data

async def data_fetch(assetsId_list, unique_signalIds):
    global df_data
    headers = get_header("tenantId", "tata00NVky")
    # Get the current time
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(minutes=10)

    # Add 10 minutes to the current time
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())
    
    tasks = []
    dict = {} 
    async with aiohttp.ClientSession() as session:
        for i in assetsId_list:
            dict[i] = {
                'ts': '',
                'q': ''
            }
            for j in unique_signalIds:
                dict[i][j]= ""
                params = {
                    'signalIds': j,
                    'assetIds': i,
                    'startTime': start_timestamp,
                    'endTime': end_timestamp,
                    'aggregation': 'AVG_10M'
                }
                tasks.append(send_request(session, signal_url, headers, params))
        allres = await asyncio.gather(*tasks)

        for res in allres:
            asset = res['assetIds'][0]
            result = asset['result']
            asset_id = asset['assetId']
            # print(asset_id)
            parameter = next(iter(result))
            signal_id = list(result.keys())[0]
            data = [(data['ts'], data['q'], data['v']) for data in result[parameter]]
            if(len(data) != 0 ):
                dict[asset_id]['ts'] = data[0][0]
                dict[asset_id]['q'] = data[0][1]
                dict[asset_id][signal_id] = data[0][2]
        
        df_list = []
        for asset_id, values in dict.items():
            df_values = {'assest_id': asset_id, **values}
            df_list.append(df_values)

        df = pd.DataFrame(df_list)
        return df


# get unique single id and assets ids
signal_res = signal_id_response(input_para)
assetsIds = signal_res[0]
chunk_size= 10
chunks = [assetsIds[i:i + chunk_size] for i in range(0, len(assetsIds), chunk_size)]
final_df = pd.DataFrame()
loop = asyncio.get_event_loop()
for i, assest_ids in enumerate(chunks):
    if(i != 0): 
        time.sleep(1.2)
    df = loop.run_until_complete(data_fetch(assest_ids, signal_res[1]))
    final_df = pd.concat([final_df, df], axis=0)
    print(assest_ids)
loop.close()
end_time = time.time()
total_time = end_time - start_time
print("total time taken",total_time)
print(final_df)
 



