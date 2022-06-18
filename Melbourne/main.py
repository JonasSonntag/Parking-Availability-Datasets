import json
import os
import pandas as pd
from itertools import combinations
import re
import math
import datetime
import osmnx as ox
import matplotlib.pyplot as plt
import seaborn as sns

from load_melbourne_data import prepare_melbourne_sensor_data, prepare_melbourne_parking_bays, prepare_melbourne_road_segment_info


def main():
    # path=os.environ['input_container_path']
    sensor_data = prepare_melbourne_sensor_data("2019")
    sensor_data = sensor_data.drop(
        columns=['DeviceId', 'SignPlateID', 'AreaName', 'StreetId', 'StreetName', 'BetweenStreet1ID', 'BetweenStreet1',
                 'BetweenStreet2ID',
                 'BetweenStreet2', 'SideOfStreet', 'SideOfStreetCode', 'SideName'])
    sensor_data["DurationMinutes"] = sensor_data["DurationMinutes"].astype("float64")
    sensor_data["BayId"] = sensor_data["BayId"].astype("category")
    sensor_data["InViolation"] = sensor_data["InViolation"].astype("int8")
    sensor_data["VehiclePresent"] = sensor_data["VehiclePresent"].astype("int8")

    bay_locations = prepare_melbourne_parking_bays(melbourne_sensordata)
    road_seg_info = prepare_melbourne_road_segment_info(bay_locations)

    road_seg_info["capacity"] = road_seg_info["capacity"].astype("float32")
    road_seg_info["sensor_coverage"] = road_seg_info["sensor_coverage"].astype("float32")
    road_seg_info["length"] = road_seg_info["length"].astype("float16")
    road_seg_info["sensor_markers_list"] = road_seg_info.sensor_markers.apply(
        lambda x: x.replace("'", "").strip('][').split(' '))

    # create new row for each sensor_marker
    new = []
    for idx, row in road_seg_info.iterrows():
        for bay in row.sensor_markers_list:
            tmp_row = row.copy()
            tmp_row["sensor_marker"] = bay
            new.append(tmp_row)
            del tmp_row
    road_seg_info_per_sensor = pd.DataFrame(pd.concat(new, axis=1)).transpose().reset_index().drop(columns=["index"])

    road_seg_info_sensorized = road_seg_info_per_sensor[
        road_seg_info_per_sensor.sensor_coverage == 1.0].reset_index().drop(columns=["index"])
    # list of bays on fully sensorized streets
    bays_from_sensorized_streets = []
    for entry in road_seg_info_sensorized.sensor_markers_list:
        bays_from_sensorized_streets.extend(entry)
    sensor_data_sensorized = sensor_data[
        sensor_data.StreetMarker.isin(bays_from_sensorized_streets)].reset_index().drop(columns=["index"])
    sensor_data_sensorized.ArrivalTime = pd.to_datetime(sensor_data_sensorized.ArrivalTime)
    sensor_data_sensorized["Date"] = sensor_data_sensorized.ArrivalTime.dt.date

    all_days = pd.date_range(start=pd.to_datetime("01.01.2019"), end=pd.to_datetime("31.05.2020"))
    all_sensors = sensor_data_sensorized.StreetMarker.unique()
    all_days_all_sensors = pd.DataFrame(
        index=pd.MultiIndex.from_product([all_days, all_sensors], names=["Date", "StreetMarker"])).reset_index()

    sensor_data_sensorized.Date = pd.to_datetime(sensor_data_sensorized.Date)
    all_days_all_sensors = pd.merge(all_days_all_sensors, sensor_data_sensorized, on=["Date", "StreetMarker"],
                                    how="left")
    days_with_working_sensors = all_days_all_sensors[all_days_all_sensors.ArrivalTime.notna()].reset_index()[
        ["Date", "StreetMarker"]].drop_duplicates().reset_index(drop=True)
    days_with_working_sensors_streets = \
    pd.merge(days_with_working_sensors, road_seg_info_sensorized[["rd_seg_id", "sensor_marker"]],
             left_on="StreetMarker", right_on="sensor_marker")[["Date", "StreetMarker", "rd_seg_id"]]
    days_with_working_sensors_streets_only_streets = days_with_working_sensors_streets[
        ["Date", "rd_seg_id"]].drop_duplicates()
    sensor_data_sensorized = sensor_data_sensorized[sensor_data_sensorized.VehiclePresent == 1]
    sensor_data_road_info_merge = pd.merge(sensor_data_sensorized, road_seg_info_sensorized, left_on="StreetMarker",
                                           right_on="sensor_marker", how="left")
    sensor_data_road_info_merge.ArrivalTime = pd.to_datetime(sensor_data_road_info_merge.ArrivalTime)
    sensor_data_road_info_merge.DepartureTime = pd.to_datetime(sensor_data_road_info_merge.DepartureTime)
    sensor_data_road_info_merge_sorted_by_time = sensor_data_road_info_merge.sort_values(
        by=["ArrivalTime"]).reset_index().drop(columns="index")
    # downsampling: take only one time point per 5min interval
    time_boxes = pd.date_range(start="2019-01-01 00:00:00", end="2020-05-31 23:59:00", freq="1T")
    rand_samples = [np.random.choice(range(x, x + 5)) for x in range(0, len(time_boxes), 5)]
    time_boxes_sampled = time_boxes[rand_samples]
    # cartesian product of time boxes and sensors
    time_boxes_with_sensor_markers_new = pd.DataFrame(index=pd.MultiIndex.from_product(
        [time_boxes_sampled, road_seg_info_sensorized["sensor_marker"].values])).reset_index()
    time_boxes_with_sensor_markers_new.columns = ["start_intervall", "sensor_marker"]
    time_boxes_with_sensor_markers_new = pd.merge(time_boxes_with_sensor_markers_new, road_seg_info_sensorized[
        ["sensor_marker", "length", "capacity", "rd_seg_id"]], on="sensor_marker")
    time_boxes_with_sensor_markers_new["weekday"] = time_boxes_with_sensor_markers_new.start_intervall.apply(
        lambda x: x.day)
    time_boxes_with_sensor_markers_new["hour"] = time_boxes_with_sensor_markers_new.start_intervall.apply(
        lambda x: x.hour)

    parking_intervalls = create_parking_intervalls(sensor_data_road_info_merge_sorted_by_time)
    merge_intervalls = pd.merge(time_boxes_with_sensor_markers_new, parking_intervalls,
                                on=["sensor_marker", "start_intervall"], how="left")
    # filter for only day hours
    merge_intervalls_6am_8pm = merge_intervalls[merge_intervalls.start_intervall.dt.hour.isin(range(6, 21))]
    all_roads = road_seg_info_per_sensor[road_seg_info_per_sensor.sensor_coverage == 1].drop_duplicates(
        subset='rd_seg_id')
    sensor_capa = pd.read_csv('sensor_capa.csv', index_col=0)
    sensor_capa.set_index("sensor_marker", inplace=True)
    all_sensors = all_roads.explode("sensor_markers_list").sensor_markers_list
    missing_sensors = pd.DataFrame(index=list(set(all_sensors) - set(sensor_capa.index)))
    missing_sensors['capa'] = 1
    sensor_capa = pd.concat([sensor_capa, missing_sensors], axis=0)
    all_roads['capacity'] = [np.sum(sensor_capa.loc[x.sensor_markers_list].values) for _, x in all_roads.iterrows()]
    merge_intervalls_6am_8pm = merge_intervalls_6am_8pm.rename({'capacity': 'capa'}, axis=1)
    print('start saving data to s3')
    merge_intervalls_6am_8pm.to_csv('melbourne_sensor_intervals.csv')


if __name__ == '__main__':
    main()