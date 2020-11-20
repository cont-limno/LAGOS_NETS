# a simple script, checked, no test cases
import csv
import os


scale = 'Nicole'#'Nicole' #  medium resolution or other resolution (Nicole, Hr)
results_dir_stream = 'All/'


if scale == 'Med':
    #  graph result file
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                     + results_dir_stream + '/PureConnection/'
    chainIDfilepath = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                  + results_dir_stream + '/LakeChain/MedLakeChainIDs.csv'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                  + results_dir_stream + 'Metrics/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                     + results_dir_stream + '/PureConnection/'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                  + results_dir_stream + 'Metrics/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

pairs = {}
for filename in os.listdir(connection_dir):
    filepath = connection_dir + filename
    with open(filepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['Lake1'] not in pairs:
                pairs[row['Lake1']] = {}
            pairs[row['Lake1']][row['Lake2']] = float(row['LENGTHKM'])

print('Pairs constructed.')
IDsLakes = {}
with open(chainIDfilepath, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        if row['chain_id'] not in IDsLakes:
            IDsLakes[row['chain_id']] = []
        IDsLakes[row['chain_id']].append(row['lagoslakeid'])


# lake numbers in a chain
lake_number = {}
for id in IDsLakes:
    lake_number[id] = len(IDsLakes[id])

# calculate average distance
averge_dist = {}
for id in IDsLakes:
    count = 0
    totaldis = 0.0
    lakesinchain = IDsLakes[id]
    for i in range(len(lakesinchain)):
        for j in range(len(lakesinchain)):
            if lakesinchain[i] in pairs[lakesinchain[j]]:
                count += 1
                totaldis += pairs[lakesinchain[j]][lakesinchain[i]]
    averge_dist[id] = totaldis/count


#  save result
with open(results_dir + 'othermetrics.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['chain_id', 'lake_number', 'avg_distant'])
    for id in lake_number:
        writer.writerow([id, lake_number[id], averge_dist[id]])


