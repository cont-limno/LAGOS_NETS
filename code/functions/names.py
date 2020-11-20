import os


def get_names(scale, Type):
    if Type == 'All':
        results_dir_stream = 'All/'
    elif Type == 'Upstream':
        results_dir_stream = 'Upstream/'
    elif Type == 'Downstream':
        results_dir_stream = 'Downstream/'

    if scale == 'Med':
        #  graph result file
        results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' + results_dir_stream
        if not os.path.isdir(results_dir):
            os.makedirs(results_dir)
        intermediate_file_path = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/intermediate_tables/Med/'
        if not os.path.isdir(intermediate_file_path):
            os.makedirs(intermediate_file_path)
        # load flow file and find the connections for each lake
        lagosIDpath = intermediate_file_path + 'lagosid_COMID.csv'  # table to store matched IDs
        ID_flow = 'COMID'  # or 'Permanent_Identifier'
        weightPath = intermediate_file_path + 'Length.csv'  # table to store weight data
        weightName = 'LENGTHKM'  # the name for weight
        FCodePath = intermediate_file_path + 'FCode.csv'
        FCodeName = 'FCODE'
        Flowdir = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/flow/'  # the tables for From To data
        FromName = 'FROMCOMID'
        ToName = 'TOCOMID'

    else:  # high resolution data provided by Nicole
        # For Nicole's data,
        #   'NHDFlow' contains the from to data
        #   'NHDFlowline' is used to match the ID of flows with lagos
        #   'NHDFlowline2' contains the Fcode, length information

        results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' + results_dir_stream
        if not os.path.isdir(results_dir):
            os.makedirs(results_dir)
        intermediate_file_path = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/intermediate_tables/Nicole/'
        if not os.path.isdir(intermediate_file_path):
            os.makedirs(intermediate_file_path)
        # load flow file and find the connections for each lake
        lagosIDpath = intermediate_file_path + 'lagosid_PID.csv'  # or 'lagosid_PID'
        ID_flow = 'Permanent_Identifier'  # or 'Permanent_Identifier'
        weightPath = intermediate_file_path + 'Length.csv'
        weightName = 'LengthKM'
        FCodePath = intermediate_file_path + 'FCode.csv'
        FCodeName = 'FCode'
        Flowdir = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/NHDFlow/'
        FromName = 'From_Permanent_Identifier'
        ToName = 'To_Permanent_Identifier'

    PairPath = intermediate_file_path + 'PairsUS_' + Type + '.csv'
    LAGOSName = 'LAGOS'  # a name to added to the end of lagosid
    # in case lagosid has conflict with the stream IDs

    return lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
           FCodeName, Flowdir, FromName, ToName, LAGOSName, results_dir, PairPath

def get_names_lake_order(scale, Type):
    """function to get names only when calculate lake orders"""
    lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
    FCodeName, Flowdir, FromName, ToName, LAGOSName, \
    results_dir, PairPath = get_names(scale, Type)
    intermediate_file_path = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/intermediate_tables/Med/'
    OrderPath = intermediate_file_path + 'stream_order.csv'
    return lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
           FCodeName, Flowdir, FromName, ToName, LAGOSName, \
           results_dir, OrderPath, PairPath


if __name__ == "__main__":
    pass
