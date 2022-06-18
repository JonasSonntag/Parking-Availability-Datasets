from get_groundtruth_data import preprocess_study_data
from get_transaction_data import gen_seattle_transaction_data

def main():
    """
    Query open data portal to receive labeled groundtrtuh data of seattle
    Saves parking transasction data as graph with distance matrix between the locations
    Can be enriched with parking meter transactions and other features like weather etc.
    :return:
    """
    groundtruth_data = preprocess_study_data()
    groundtruth_data['availability_label'] = groundtruth_data.total_vehicle_count < groundtruth_data.parking_spaces
    groundtruth_data.to_csv("seattle_groundtruth.csv")
    gen_seattle_transaction_data(area="Uptown")


if __name__ =="__main__":
    main()