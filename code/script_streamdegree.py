import os
import csv

# did not test or check

scale = 'Med' #  'Med' or other

# out degree
if scale == 'Med':
    #  graph result file
    connection_dir_down = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/PureConnection/streams/'
    results_path_down = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/Outdegree_stream.csv'

    connection_dir_up = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Upstream/PureConnection/streams/'
    results_path_up = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Upstream/Indegree_stream.csv'


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir_down = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/PureConnection/streams/'
    results_path_down = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/Outdegree_stream.csv'

    connection_dir_up = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Upstream/PureConnection/streams/'
    results_path_up = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Upstream/Indegree_stream.csv'


def count_degree(connection_dir, results_path):
    pairs = {}
    for filename in os.listdir(connection_dir):
        filepath = connection_dir + filename
        with open(filepath, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                if row['stream(COMID)'] not in pairs:
                    pairs[row['stream(COMID)']] = []
                pairs[row['stream(COMID)']].append(row['Lake(lagosid)'])
    degree = {}
    for i in pairs:
        degree[i] = len(pairs[i])

    with open(results_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['stream(COMID)', 'Degree'])
        for key in degree:
            writer.writerow([key, degree[key]])

count_degree(connection_dir_down, results_path_down)
count_degree(connection_dir_up, results_path_up)