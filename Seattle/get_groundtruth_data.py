import numpy as np
import pandas as pd
from sodapy import Socrata
import requests
import os
import matplotlib.pyplot as plt
import time
import itertools

def get_open_seattle_data(api_id, num_retries=5):
    client = Socrata("data.seattle.gov", None)
    #Try multiple times due do timeout error may occur
    for i in range(num_retries):
        try:
            data = client.get(api_id,limit=10000000)
            break
        except:
            print('Error with connection: trying again')
            if i==(num_retries-1):
                return False
    data=pd.DataFrame.from_records(data)
    return data

def preprocess_study_data():
    parking_study = get_open_seattle_data('7jzm-ucez')
    parking_study.drop_duplicates(inplace=True)
    parking_study.dropna(subset=['total_vehicle_count', 'parking_spaces', 'time_stamp', 'study_area'], how='any',
                         inplace=True)
    areas = parking_study.study_area.unique().tolist()
    splited = [x.split(' ') for x in areas]
    differences = [(set(x).difference(set(y)), x, y) for x, y in itertools.combinations(splited, 2)]
    mask = [len(x[0].difference('2017', '-', 'Annual', 'Study', 'Spring', 'Summer', 'summer', 'Saturday')) == 0 for x in
            differences]
    replace_dicts = [{' '.join(x[2]): ' '.join(x[1])} for x, y in zip(differences, mask) if y]
    replace_dict = dict()
    for dic in replace_dicts:

        if [('Locks' in key) and (not 'Locks' in val) for key, val in dic.items()][0]:
            continue
        elif [('Uptown Triangle' in key) for key, val in dic.items()][0]:
            continue
        else:
            replace_dict.update(dic)
    replace_dict.update({'12th Avenue': '12th Ave'})
    replace_dict.update({'Chinatown ID 2017': 'Chinatown/ID'})
    replace_dict.update({'12th Ave 2017 Annual Study': '12th Ave'})
    replace_dict.update({'Green Lake': 'Greenlake'})
    for i in range(2):
        parking_study.study_area.replace(replace_dict, inplace=True)

    parking_study['total_vehicle_count'] = parking_study['total_vehicle_count'].map(int)
    parking_study['parking_spaces'] = parking_study['parking_spaces'].map(int)
    parking_study['occupancy'] = parking_study['total_vehicle_count'] / parking_study['parking_spaces']
    parking_study.loc[~np.isfinite(parking_study['occupancy']), 'occupancy'] = np.nan
    parking_study['occupancy'].fillna(0, inplace=True)
    parking_study.time_stamp = pd.to_datetime(parking_study.time_stamp)
    parking_study.date_time = pd.to_datetime(parking_study.date_time)
    tms = [row.date_time if np.abs(row.time_stamp.hour - row.date_time.hour) > 1 else row.time_stamp for _, row in
           parking_study.iterrows()]

    parking_study.time_stamp.replace(tms, inplace=True)
    parking_study.set_index('time_stamp', inplace=True)
    parking_study.index = parking_study.index.round('5T')
    parking_study.reset_index(inplace=True)
    parking_study.time_stamp = tms
    parking_study.set_index('time_stamp', inplace=True)
    parking_study.index = parking_study.index.round('5T')
    groundtruth = parking_study[['elmntkey', 'total_vehicle_count', 'study_area', 'parking_spaces', 'occupancy']]
    groundtruth = groundtruth.reset_index().set_index('elmntkey')
    return groundtruth
