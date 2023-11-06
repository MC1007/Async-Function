import requests
import pandas as pd
import json
import datetime
from pytz import timezone
import numpy as np

# from token_generation import token_generator_with_token1_input

# token_generator_with_token1_input()


tag_list = pd.read_excel(
    f"C:/Users/intern-mukesh/Downloads/Canonical_signal_ids_1.xlsx")
print("1111111111111111111")
mylist = tag_list['welspun'].dropna().tolist()


def signal_id_response():
    with open("access_token2.txt", "r") as txt_file:
        access_token_2 = txt_file.read().strip()

    url = "https://api-srvs.rendigital.apps.ge.com/machinedata/v1/realtime-signals"
    headers = {
        "Authorization": f"Bearer {access_token_2}",
        "tatatenant": "tata00NVky"
    }

    params_3 = {
        'sourceIds': 'rojmal_inox ',
        'signalIds': mylist

    }
    response_3 = requests.get(url, headers=headers,
                              params=params_3, verify=False)
    # print(response_3)
    # data = response.json()
    response_3_data = response_3.json()
    # print(response_3_data)

    #     break
    print(response_3_data)
    json_data = response_3_data  # Extract the signal data
    print(json_data)
    signal_data = json_data['signalData']
    print(signal_data)
    # Convert to DataFrame

    df = pd.DataFrame(signal_data)
    df = df[response_3_data['signalData'][0].keys()]
    ist = timezone('Asia/Kolkata')
    # Convert the "loggedTimeStamp" column to date and time format
    df['loggedTimeStamp'] = pd.to_datetime(df['loggedTimeStamp'], unit='s')

    # Convert the time zone to IST
    df['loggedTimeStamp'] = df['loggedTimeStamp'].dt.tz_localize(
        'UTC').dt.tz_convert(ist)

    # Change the format of the "loggedTimeStamp" column
    df['loggedTimeStamp'] = df['loggedTimeStamp'].dt.strftime(
        '%d-%m-%Y %H:%M:%S')

    # Convert the time zone to IST
    df['machineTimestamp'] = pd.to_datetime(df['machineTimestamp'], unit='s')
    df['machineTimestamp'] = df['machineTimestamp'].dt.tz_localize(
        'UTC').dt.tz_convert(ist)

    # Change the format of the "loggedTimeStamp" column
    df['machineTimestamp'] = df['machineTimestamp'].dt.strftime(
        '%d-%m-%Y %H:%M:%S')

    df['sourceTimestamp'] = pd.to_datetime(df['sourceTimestamp'], unit='s')
    df['sourceTimestamp'] = df['sourceTimestamp'].dt.tz_localize(
        'UTC').dt.tz_convert(ist)

    # Change the format of the "loggedTimeStamp" column
    df['sourceTimestamp'] = df['sourceTimestamp'].dt.strftime(
        '%d-%m-%Y %H:%M:%S')
    unique_signalIds = df['signalId'].unique()
    not_responding = []
    for element in mylist:
        if element not in unique_signalIds:
            not_responding.append(element)
    assetsId_list = df['assetId'].unique()
    # print(assetsId_list)
    # print(unique_signalIds)
    return assetsId_list, unique_signalIds
    # time_stamp(df)


a = signal_id_response()


df_data = pd.DataFrame()
data_list = []
ids = []


def data_fetch(assetsId_list, unique_signalIds):
    global df_data
    with open("access_token2.txt", "r") as txt_file:
        access_token_2 = txt_file.read().strip()
    # assetsId_list=assetsId_list[:3]
    url = "https://api-srvs.rendigital.apps.ge.com/machinedata/v1/historical-signals"
    headers = {
        "Authorization": f"Bearer {access_token_2}",
        "tenantId": "tata00NVky"}

    response_data = []
    for i in assetsId_list[:2]:
        print(i)
        for j in unique_signalIds[:2]:
            print(j)
            params_3 = {
                'signalIds': j,
                'assetIds': i,
                'startTime': '1654041600',
                'endTime': '1664668200',
                'aggregation': 'AVG_10M'
            }
        #     'sourceIds':'pratapgarh_gamesa ',
        #     'signalIds':'WNAC.WdDir',
        #     'assetIds':'100261574',
        #     'startTime':'1654041600',
        #     'aggregation':'AVG_10M',
        #     'endTime':'1664668799'
        #       'sourceIds':c,
        #          "signalIds":mylist,

            response_3 = requests.get(
                url, headers=headers, params=params_3, verify=False)
            response_3_data = response_3.json()
            asset = response_3_data['assetIds'][0]
            data = asset['result']
            asset_id = asset['assetId']
            key = list(data.keys())[0]
            column_name = key
            values = [item['v'] for item in data[key]]
            parameter = next(iter(data))

            if (asset_id not in ids):
                timestamp_values = [
                    (asset_id, data['ts'], data['q'], data['v']) for data in data[parameter]]
                df_test = pd.DataFrame(timestamp_values, columns=[
                                       'asset_id', 'ts', 'q', column_name])
                ids.append(asset_id)

            else:
                signalCol = [(data['v']) for data in data[parameter]]
                df_test[column_name] = pd.Series(signalCol)

        df_data = pd.concat([df_data, df_test], axis=0)
    print(df_data)

    print("DONE FINALLY")


data_fetch(a[0], a[1])
