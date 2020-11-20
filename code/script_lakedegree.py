import os
import csv

# did not test or check

scale = 'Nicole' #  'Med' or other

# out degree
if scale == 'Med':
    #  graph result file
    connection_dir_down = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/PureConnection/'
    results_path_down = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/Outdegree.csv'

    connection_dir_up = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Upstream/PureConnection/'
    results_path_up = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Upstream/Indegree.csv'


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir_down = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/PureConnection/'
    results_path_down = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/Outdegree.csv'

    connection_dir_up = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Upstream/PureConnection/'
    results_path_up = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Upstream/Indegree.csv'


def count_degree(connection_dir, results_path):
    pairs = {}
    for filename in os.listdir(connection_dir):
        filepath = connection_dir + filename
        with open(filepath, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                if row['Lake1'] not in pairs:
                    pairs[row['Lake1']] = []
                pairs[row['Lake1']].append(row['Lake2'])
    degree = {}
    for i in pairs:
        degree[i] = len(pairs[i])

    with open(results_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['lagosid', 'Degree'])
        for key in degree:
            writer.writerow([key, degree[key]])

count_degree(connection_dir_down, results_path_down)
count_degree(connection_dir_up, results_path_up)