import os
import csv
from traversal_dij.functions import NetworkNumberAlg

# checked by test cases

scale = 'Nicole' #  'Med' or other


if scale == 'Med':
    #  graph result file
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/PureConnectionNoCircle/'
    results_dir = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/LakeNetworkNumber/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/PureConnectionNoCircle/'
    results_dir = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/LakeNetworkNumber/'

    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

pairs = {}
all_lakes = set()
lakes_downstream = set()

filepath = connection_dir + 'PairsDown.csv'
with open(filepath, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        if row['From'] not in pairs:
            pairs[row['From']] = []
        pairs[row['From']].append(row['To'])
        all_lakes.add(row['From'])
        all_lakes.add(row['To'])
        lakes_downstream.add(row['To'])

start_lakes = all_lakes - lakes_downstream
print('Pairs constructed.')

LakeNumber = NetworkNumberAlg.Network(pairs, start_lakes).CalNumber()

with open(results_dir + 'NetworkNumber.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['lagosid', 'LakeNetworkNumber'])
    for key in LakeNumber:
        writer.writerow([key, LakeNumber[key]])

