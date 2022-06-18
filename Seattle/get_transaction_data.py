import pandas as pd
from sodapy import Socrata
import pandas as pd
import numpy as np
from math import sin, cos, sqrt, atan2, radians
import datetime


def get_gps(obj):
    # temp=obj['coordinates']
    gps = temp.split(' ')
    gps[0] = float(gps[0])
    gps[1] = float(gps[1])
    return gps


# Return distance in kilometers from coordinates
def dist_in_km(x, y):
    R = 6373.0
    lat1 = radians(x[1])
    lon1 = radians(x[0])
    lat2 = radians(y[1])
    lon2 = radians(y[0])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def gen_seattle_transaction_data(num_retries=10, limit=20000000, area='Uptown'):
    client = Socrata("data.seattle.gov", None)
    # Try multiple times due do timeout error may occur
    for i in range(num_retries):
        try:
            seattle_uptown = client.get("6yaw-2m8q", paidparkingarea=area, limit=limit)
            break
        except:
            print('Error with connection: trying again')
            if i == (num_retries - 1):
                return False
    # -----------General Preprocessing
    seattle = pd.DataFrame.from_records(seattle_uptown)
    seattle.paidoccupancy = seattle.paidoccupancy.apply(lambda x: float(x))
    seattle.parkingspacecount = seattle.parkingspacecount.apply(lambda x: float(x))
    seattle.location = seattle.location.apply(lambda x: x['coordinates'])
    # Build primary key for location identifier
    # -------------Build distance matrix
    # get all locations
    locations = seattle.groupby(['sourceelementkey']).last()
    # get corresponding coordinates
    coordinates = locations.location
    dist_mat = [_ for _ in map(
        lambda yar: [_ for _ in map(
            lambda var: dist_in_km(x=coordinates[yar], y=coordinates[var]),
            range(len(coordinates)))],
        range(len(coordinates)))]

    dist_mat = np.asarray(dist_mat)
    dist_mat = np.vstack(dist_mat)
    np.savetxt('./Data/Seattle_{}-dist_mat.csv'.format(area), dist_mat)

    # ----------Aggregate data to 5 minute bins
    seattle.occupancydatetime = pd.to_datetime(seattle.occupancydatetime)
    trans_start = seattle.occupancydatetime.min()
    trans_end = seattle.occupancydatetime.max()
    time_delta = trans_end - trans_start
    index = [trans_start + datetime.timedelta(minutes=5 * n) for n in
             range(0, int(time_delta.total_seconds() / 300) + 1)]

    V = pd.DataFrame(index=index)
    data = seattle[['occupancydatetime', 'paidoccupancy', 'parkingspacecount', 'sourceelementkey']]
    data['occupancy'] = data.paidoccupancy / data.parkingspacecount
    for block in data.sourceelementkey.unique():
        df_location = data[data.sourceelementkey == block].reset_index(drop=True).drop(
            ['paidoccupancy', 'parkingspacecount', 'sourceelementkey'], axis=1)
        df_location = df_location.set_index('occupancydatetime')
        df_location.columns = [block]
        df_location = df_location.sort_index()
        df_location = df_location.resample('5Min').mean()
        df_location = df_location[~df_location.index.duplicated(keep='first')]
        V = pd.concat([V, df_location], axis=1)
    V.to_csv('./Data/Seattle_{}-node_features.csv'.format(area))
    return True
