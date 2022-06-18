from sodapy import Socrata
import pandas as pd
import numpy as np
from shapely.geometry import shape, box
from shapely.ops import unary_union
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
import boto3
import math
import seaborn as sns
from geolocation_helper import calculate_diagonal_of_polygon
from here_maps_helper import *


def api_name_for_year(year):
    mapping = {
        "2017": "u9sa-j86i",
        "2018": "5532-ig9r",
        "2019": "7pgd-bdf2",
        "2020": "4n3a-s6rn"
    }
    return mapping[year]


def load_open_data(client_name, data_id, limit=100000000):
    client = Socrata(client_name, None)
    data = client.get(data_id, limit=limit)
    data = pd.DataFrame.from_records(data)
    return data


def prepare_plots(s3_client, target_s3_bucket, target_s3_key, road_segment, bay_locations):
    # Plot the capacity distribution
    plt.hist(road_segment['capacity'])
    plt.title('Distribution of parking bays per street segment')
    plt.savefig('Distribution of parking bays per street segment')
    s3_client.upload_file('Distribution of parking bays per street segment.png', target_s3_bucket,
                          target_s3_key + "/Distribution of parking bays per street segment.png")
    # Plot the sensor coverage distribution
    plt.close()
    plt.hist(road_segment['sensor_coverage'])
    plt.title('Distribution of sensor coverage per street')
    plt.savefig('Distribution of sensor coverage per street')
    s3_client.upload_file('Distribution of sensor coverage per street.png', target_s3_bucket,
                          target_s3_key + "/Distribution of sensor coverage per street.png")
    plt.close()
    # Plot the sensor coverage distribution given at least one sensor
    plt.hist(road_segment['sensor_coverage'][road_segment.sensor_coverage > 0])
    plt.title('Distribution of sensor coverage per street given at least one sensor')
    plt.savefig('Distribution of sensor coverage per street given at least one sensor')
    s3_client.upload_file('Distribution of sensor coverage per street given at least one sensor.png', target_s3_bucket,
                          target_s3_key + "/Distribution of sensor coverage per street given at least one sensor.png")
    # Plot all parking bays sensorized vs unsensorized
    m = folium.Map(location=[bay_locations.iloc[0].geometry.centroid.y, bay_locations.iloc[0].geometry.centroid.x],
                   zoom_start=12, tiles="OpenStreetMap")
    folium.Choropleth(gpd.GeoDataFrame(bay_locations[bay_locations.streetmarker.isna()].geometry).set_crs(epsg=4326),
                      line_weight=4, line_color="red").add_to(m)
    folium.Choropleth(gpd.GeoDataFrame(bay_locations[bay_locations.streetmarker.notna()].geometry).set_crs(epsg=4326),
                      line_weight=4, line_color="green").add_to(m)
    m.save('melbourne_coverage.html')
    s3_client.upload_file('melbourne_coverage.html', target_s3_bucket, target_s3_key + "/melbourne_coverage.html")


def create_heatmap_per_day_hour(data_df):
    df = data_df.groupby(by=["hour", "weekday"]).apply(lambda x: x.available_label.mean()).reset_index(
        name="available_label")

    plt.figure()
    plot = sns.scatterplot(data=df, x="hour", y="weekday", hue="available_label")
    plt.legend(bbox_to_anchor=(1.01, 1), borderaxespad=0)
    plt.title("Availability in Melbourne 2019-2020")
    fig = plot.get_figure()
    fig.savefig('groundtruth_melbourne_hours_2019_2020.png', bbox_inches='tight')


def prepare_melbourne_sensor_data(years):
    s3_client = boto3.client('s3')

    melbourne_sensordata = []
    years_string = ""
    for year in years:
        print("Loading sensor data from year {}...".format(year))

        # Read sensor data for year
        melbourne_sensors = load_open_data("data.melbourne.vic.gov.au", api_name_for_year(year))
        melbourne_sensors.arrivaltime = pd.to_datetime(melbourne_sensors.arrivaltime)
        melbourne_sensors.departuretime = pd.to_datetime(melbourne_sensors.departuretime)

        # upload to S3
        sensor_filename = "melbourne_sensor_data_{}.csv".format(year)
        melbourne_sensors.to_csv(sensor_filename)
        #s3_client.upload_file(sensor_filename, target_s3_bucket, target_s3_key + sensor_filename)

        print("Successfully stored on S3.")

        year_string = year_string + "_" + str(year)
        melbourne_sensordata.append(melbourne_sensors)

    # concat sensor data for all requested years
    melbourne_sensordata = pd.concat(melbourne_sensordata)

    # upload to S3
    sensor_filename = "melbourne_sensor_data_{}.csv".format(year_string)
    melbourne_sensordata.to_csv(sensor_filename)
    #s3_client.upload_file(sensor_filename, target_s3_bucket, target_s3_key + sensor_filename)

    print("Successfully stored the concatenated data on S3.")

    return melbourne_sensordata


def prepare_melbourne_parking_bays(melbourne_sensors):
    s3_client = boto3.client('s3')

    print("Loading parking bay information...")

    # Read data about geographic information of parking bays (sensor and non-sensor parking)
    bay_locations = load_open_data("data.melbourne.vic.gov.au", "wuf8-susg")
    bay_locations['geometry'] = bay_locations['the_geom'].apply(lambda x: shape(x))

    ### Merge with sensors to get an overview about how many bays are sensorized
    bay_locations = bay_locations.merge(melbourne_sensors['streetmarker'].drop_duplicates(), left_on='marker_id',
                                        right_on='streetmarker', how='left')
    bay_locations.to_csv('melbourne_all_parking_bays.csv')
    #s3_client.upload_file('melbourne_all_parking_bays.csv', target_s3_bucket,
                          #target_s3_key + "/melbourne_all_parking_bays.csv")

    #print("Successfully stored on S3.")

    return bay_locations


def prepare_melbourne_raw_data(target_s3_bucket, target_s3_key, years):
    if not years:
        years = ["2019", "2020"]

    melbourne_sensordata = prepare_melbourne_sensor_data(target_s3_bucket, target_s3_key, years)
    bay_locations = prepare_melbourne_parking_bays(melbourne_sensordata, target_s3_bucket, target_s3_key)
    road_seg_rep = prepare_melbourne_road_segment_info(bay_locations, target_s3_bucket, target_s3_key)

    # TODO: concatenate datasets

    # TODO: prepare the code so that the actual API call is only once (separate API call to rest of the code)

    return melbourne_sensordata, bay_locations


def prepare_melbourne_road_segment_info(bay_locations):
    s3_client = boto3.client('s3')

    # Look at a street segment -level representation of data
    road_seg_rep = bay_locations.groupby('rd_seg_id').apply(lambda x: x.bay_id.dropna().unique())
    road_seg_rep = road_seg_rep.reset_index(name='bay_ids')
    road_seg_rep['capacity'] = road_seg_rep.bay_ids.apply(len)

    sensor_names = bay_locations.groupby('rd_seg_id').apply(lambda x: x.streetmarker.dropna().unique()).reset_index(
        name='sensor_markers')
    road_seg_rep = road_seg_rep.merge(sensor_names, on='rd_seg_id', how='left')
    road_seg_rep['sensor_coverage'] = road_seg_rep['sensor_markers'].apply(len) / road_seg_rep['bay_ids'].apply(len)
    bay_locations.set_index('bay_id', inplace=True)
    road_seg_rep['geometry'] = road_seg_rep.apply(lambda x: unary_union(bay_locations.loc[x.bay_ids].geometry), axis=1)

    # calculate length of street segments
    road_seg_rep['length'] = road_seg_rep['geometry'].apply(lambda x: calculate_diagonal_of_polygon(x))

    # upload to S3
    road_seg_rep.to_csv('melbourne_road_segment_info.csv')
    #s3_client.upload_file('melbourne_road_segment_info.csv', target_s3_bucket,
                          #target_s3_key + "/melbourne_road_segment_info.csv")
    #prepare_plots(s3_client, target_s3_bucket, target_s3_key, road_seg_rep, bay_locations)

    return road_seg_rep


def create_parking_intervalls(sensor_data_road_info_merge_sorted_by_time):
    # create 1 mins intervalls when the spots where occupied
    new = []
    for idx, row in sensor_data_road_info_merge_sorted_by_time.iterrows():
        start_intervall = pd.date_range(start=row.ArrivalTime.floor("1T"), end=row.DepartureTime.ceil("1T"), freq="1T",
                                        closed="left")
        new.append(pd.DataFrame({"start_intervall": start_intervall,
                                 "sensor_marker": [row.sensor_marker] * len(start_intervall),
                                 "rd_seg_id": [row.rd_seg_id] * len(start_intervall),
                                 "occupied": [1] * len(start_intervall)}))
        del start_intervall
    return pd.concat(new, axis=0)  # parking_intervalls


def calculate_max_occupancy_per_sensor(parking_intervalls):
    parking_groupby_intervall_sensor = parking_intervalls.groupby(by=["start_intervall", "sensor_marker"]).agg("size")
    parking_groupby_intervall_sensor = pd.DataFrame(parking_groupby_intervall_sensor).reset_index()
    parking_groupby_intervall_sensor.columns = ["start_intervall", "sensor_marker", "occupied"]
    parking_groupby_intervall_sensor_max = parking_groupby_intervall_sensor.groupby(by="sensor_marker").agg(
        "max").reset_index().drop(labels="start_intervall", axis=1)
    parking_groupby_intervall_sensor_max.columns = ["sensor_marker", "max_capacity"]
    return parking_groupby_intervall_sensor_max


def calculate_capacity_per_street(road_seg_info_sensorized, parking_groupby_intervall_sensor_max):
    road_seg_info_sensorized_new_capacity = pd.merge(road_seg_info_sensorized, parking_groupby_intervall_sensor_max,
                                                     on="sensor_marker", how="left").groupby(by="rd_seg_id").agg(
        "sum").reset_index()
    road_seg_info_sensorized_updated = pd.merge(road_seg_info_sensorized, road_seg_info_sensorized_new_capacity,
                                                on="rd_seg_id", how="left").drop(columns=["capacity", "sensor_markers"])
    return road_seg_info_sensorized_updated[
        ["rd_seg_id", "max_capacity", "length", "geometry"]].drop_duplicates().reset_index().drop(columns="index")

