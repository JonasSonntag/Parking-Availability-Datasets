import numpy as np
import pandas as pd
import datetime
#Consider only data from the stury dates defined here
yang_study_dates = [datetime.date(year=2013, month=3, day=8), datetime.date(year=2013, month=3, day=15),
                    datetime.date(year=2013, month=3, day=29),
                    datetime.date(year=2013, month=4, day=5), datetime.date(year=2013, month=4, day=19)]

def main():
    sensor_data = pd.read_csv('data/SFpark_ParkingSensorData_HourlyOccupancy_20112013.csv')
    sensor_data_yang = sensor_data[sensor_data.COMM_UNKNOWN_TIME < 3600]
    ## Only civic center right now
    sensor_data_yang = sensor_data_yang[sensor_data_yang.PM_DISTRICT_NAME == 'Civic Center']
    sensor_data_yang.START_TIME_DT = pd.to_datetime(sensor_data_yang.START_TIME_DT, format="%d-%b-%Y %H:%M:%S")
    sensor_data_yang['parking_spaces'] = sensor_data_yang.TOTAL_TIME / 3600
    sensor_march13 = sensor_data_yang[
        (sensor_data_yang.START_TIME_DT.dt.year == 2013) & (sensor_data_yang.START_TIME_DT.dt.month == 3)]
    sensor_march13 = sensor_data_yang[[x in yang_study_dates for x in sensor_data_yang.START_TIME_DT.dt.date]]
    sensor_march13['DATE'] = pd.to_datetime(sensor_march13['CAL_DATE'], format="%d-%b-%Y").dt.date
    # Group by street block for finer granularity
    occupancy_df = sensor_march13.groupby(
        ['START_TIME_DT', 'DATE', 'STREET_BLOCK', 'PM_DISTRICT_NAME', 'parking_spaces']).apply(
        lambda x: x.TOTAL_OCCUPIED_TIME.sum() / 3600).reset_index(name='occupancy')
    occupancy_df.START_TIME_DT = pd.to_datetime(occupancy_df.START_TIME_DT)
    smart_meter_transactions = pd.read_csv('data/SFpark_MeterData_PaymentTransactions_Pilot_Smart_20112013.csv')
    transactions_yang = smart_meter_transactions[smart_meter_transactions.PM_DISTRICT_NAME == 'Civic Center']
    transactions_yang.DATE = pd.to_datetime(transactions_yang.DATE, format="%d-%b-%Y")
    transactions_yang = transactions_yang[
        (transactions_yang.DATE.dt.year == 2013) & (transactions_yang.DATE.dt.month == 3)]
    transactions_yang = transactions_yang[[x in yang_study_dates for x in transactions_yang.DATE.dt.date]]
    transactions_yang[['SESSION_START_DT', 'SESSION_END_DT']] = transactions_yang[
        ['SESSION_START_DT', 'SESSION_END_DT']].apply(lambda x: pd.to_datetime(x, format="%d-%b-%Y %H:%M:%S"), axis=1)
    transactions_yang['DATE'] = transactions_yang['DATE'].dt.date
    joined_df = occupancy_df.merge(transactions_yang, on=['DATE', 'STREET_BLOCK'], how='inner')
    joined_df['ongoing'] = (joined_df['SESSION_START_DT'] <= joined_df['START_TIME_DT']) & (
                joined_df['SESSION_START_DT'] >= joined_df['START_TIME_DT'])


















