from pytz import timezone
import datetime
import math
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# from sklearn.impute import SimpleImputer
# from scipy import stats
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


df = pd.read_csv(r"C:\Users\intern-mukesh\Downloads\Rojmal_inox_5_turbine.csv")
df_sheet = pd.read_excel('C:/Users/intern-mukesh/Downloads/GE_Tagdiscription.xlsx',
                         sheet_name='Inox Rojmal Tag Discription  ')


def rename_columns():
    json_dict = df_sheet.set_index('Link')['Name'].to_dict()
    for i in df.columns:
        if i in json_dict:
            df = df.rename(columns={i: json_dict[i]})
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    df.columns = df.columns.str.rstrip('_')
    # Replace missing values with meadian values
    for i in df.iloc[:, 3:]:
        df[i] = df[i].fillna(df[i].median())
    # outlier
    for i in df.columns[2:]:
        q1 = df[i].quantile(0.25)
        q3 = df[i].quantile(0.75)
        iqr = q3-q1  # Interquartile range
        fence_low = q1-1.5*iqr
        fence_high = q3+1.5*iqr
        # Replace outliers with low and high fences
        df[i] = df[i].apply(lambda x: fence_low if x < fence_low else (
            fence_high if x > fence_high else x))
    ist = timezone('Asia/Kolkata')

    df['ts'] = pd.to_datetime(df['ts'], unit='s')

    # Convert the time zone to IST
    df['ts'] = df['ts'].dt.tz_localize('UTC').dt.tz_convert(ist)

    df['date'] = df['ts'].dt.date
    df['time'] = df['ts'].dt.time
    df['month_column'] = pd.to_datetime(df['date']).dt.strftime('%B')
    df['month_column'] = pd.to_datetime(df['ts']).dt.strftime('%B')
    return df


def ideal_curve():
    wind_speed = [3.00, 3.50, 4.00, 4.50, 5.00, 5.50, 6.00, 6.50, 7.00, 7.50, 8.00, 8.50, 9.00, 9.40, 10.00, 10.50, 11.00, 11.50,
                  12.00, 12.50, 13.00, 13.50, 14.00, 14.50, 15.00, 15.50, 16.00, 17.00, 17.50, 18.00, 18.50, 19.00, 19.50, 20.00]
    Active_power = [1, 34, 77, 141, 209, 272, 373, 496, 631, 776, 954, 1137, 1340, 1509, 1723, 1853, 1935, 1978, 2004, 2017, 2031,
                    2033, 2035, 2037, 2037, 2037, 2033, 2036, 2036, 2036, 2036, 2036, 2036, 2036]
    # Actula_power = new_df['Active_power']

    # Plot the power curve
    plt.plot(wind_speed, Active_power, color='red',
             label='Ideal power curve', linewidth=2.5)
    plt.xlabel("Wind Speed (m/s)")
    plt.ylabel("Power (kW)")
    plt.title("Ideal Power Curve")
    plt.grid(True)

    x_data = df['wind_speed']
    y_data = df['active_power']
    # plt.figure(figsize=(12, 8))
    # plt.plot(x_data, y_data)
    plt.scatter(x_data, y_data)
    plt.xlabel("Wind_speed(m/s)")  # add X-axis label
    plt.ylabel("Active_power(KW)")  # add Y-axis label
    plt.title("Power_curve")  # add title
    plt.show()
