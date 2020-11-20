import os
import csv
from traversal_dij.functions import RemoveCircle

# checked by test cases
scale = 'Nicole' #  'Med' or other


if scale == 'Med':
    #  graph result file
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/PureConnection/'
    results_dir = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Med/withoutGL/Downstream/PureConnectionNoCircle/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/PureConnection/'
    results_dir = '/home/wangqi19//GPU1data/LakeNetwork/preprocess/results/' \
                     'Hr_Nicole/withoutGL/Downstream/PureConnectionNoCircle/'

    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

pairs = {}
all_lakes = set()
lakes_downstream = set()
for filename in os.listdir(connection_dir):
    print(filename)
    filepath = connection_dir + filename
    with open(filepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['Lake1'] not in pairs:
                pairs[row['Lake1']] = []
            pairs[row['Lake1']].append(row['Lake2'])

pairs_no_circle, circles = RemoveCircle.RmCircle(pairs).remove()

# save results
with open(results_dir + 'PairsDown.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['From', 'To'])
    for lakefrom in pairs_no_circle:
        lakesto = pairs_no_circle[lakefrom]
        for lake in lakesto:
            writer.writerow([lakefrom, lake])

with open(results_dir + 'CirclesRemoved.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['From', 'To'])
    for lakefrom in circles:
        lakesto = circles[lakefrom]
        for lake in lakesto:
            writer.writerow([lakefrom, lake])
