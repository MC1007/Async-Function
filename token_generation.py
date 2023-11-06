import requests
import pandas as pd
import json
import datetime
# tag_list = pd.read_excel(
#     r"C://Users//intern-mukesh//Downloads//Canonical_signal_ids.xls")
# r"C:/Users/intern-mukesh/Downloads/Canonical_signal_ids.xlsx")


def token_generator(url, headers, data, filename):
    # url = 'https://apis.rendigital.apps.ge.com/auth/RENDS/token'  # Replace with your URL
    # headers = {'Content-Type':'application/x-www-form-urlencoded'}  # Specify the JSON content type header
    # data = {'client_id':'tata-machinedata-lookup','client_secret':'3d0dca0e-cc9f-408b-a944-310ce747812e','grant_type':'client_credentials','scope':'profile'}  # Replace with your URL-encoded body data

    response = requests.post(url, headers=headers, data=data, verify=False)
    print(response.json())

    # Check the response
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        # Use the access token in the subsequent requests
    else:
        print("Request failed with status code:", response.status_code)

    # file_path = "access_token.txt"

    # Write the access token to the text file
    with open(filename, "w") as txt_file:
        txt_file.write(access_token)

    # Close the file
    txt_file.close()
    print("Token Generated!!")
    # print(access_token)


# TOKEN DETAILS FOR TOKEN 1
url1 = 'https://apis.rendigital.apps.ge.com/auth/RENDS/token'
headers1 = {'Content-Type': 'application/x-www-form-urlencoded'}
data1 = {'client_id': 'tata-machinedata-lookup', 'client_secret': '3d0dca0e-cc9f-408b-a944-310ce747812e',
         'grant_type': 'client_credentials', 'scope': 'profile'}
filename1 = 'access_token1.txt'

# TO GENERATE TOKEN 1

# token_generator(url1, headers1, data1, filename1)


def token_generator_with_token1_input():
    url2 = "https://apis.rendigital.apps.ge.com/auth/RENDS/token"

    data2 = {
        "grant_type": "urn:ietf:params:oauth:grant-type:uma-ticket",
        "audience": "tata-machinedata-lookup"
    }
    with open("access_token1.txt", "r") as txt_file:
        access_token = txt_file.read().strip()
    headers2 = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    filename2 = 'access_token2.txt'
    token_generator(url2, headers2, data2, filename2)


# TOKEN GENERATOR 2
token_generator_with_token1_input()
