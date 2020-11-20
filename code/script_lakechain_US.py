import os
import csv
from traversal_dij.functions import LakeChainAlg
import sys

# did not test?

scale = 'Nicole'#'Nicole' #  medium resolution or other resolution (Nicole, Hr)
results_dir_stream = 'All/'


if scale == 'Med':
    #  graph result file
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                     + results_dir_stream + '/PureConnection/'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                  + results_dir_stream + 'LakeChain/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                     + results_dir_stream + '/PureConnection/'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                  + results_dir_stream + 'LakeChain/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

pairs = {}
for filename in os.listdir(connection_dir):
    filepath = connection_dir + filename

    with open(filepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['Lake1'] not in pairs:
                pairs[row['Lake1']] = []
            pairs[row['Lake1']].append(row['Lake2'])
            # # # only for partial lake chain
            # if row['Lake2'] not in pairs:
            #     pairs[row['Lake2']] = []
            # pairs[row['Lake2']].append(row['Lake1'])
print('Pairs constructed.')

vertices = set(list(pairs.keys()))
for i in pairs:
    for j in pairs[i]:
        vertices.add(j)

LakeChain = LakeChainAlg.LakeChain(pairs, vertices).FindChain()
count= 0
lakes_all = set()
for i in LakeChain:
    if len(i) != 1:
        count += 1
        for j in i:
            lakes_all.add(j)

print(len(lakes_all))
print(count)

print('Chain algorithm finished.')
with open(results_dir + 'lakechain.csv', 'w', newline="") as csv_file:
    writer = csv.writer(csv_file)
    for i in LakeChain:
        if len(i) != 1:
            writer.writerows([i])

debug_point = 1


