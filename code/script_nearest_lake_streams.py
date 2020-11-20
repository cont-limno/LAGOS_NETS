import os
import csv


# did not test?

scale = 'Med'#'Nicole' #  medium resolution or other resolution (Nicole, Hr)
stream = 'Upstream'
results_dir_stream = stream + '/'


if scale == 'Med':
    #  graph result file
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                     + results_dir_stream + '/PureConnection/streams/'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Med/withoutGL/' \
                  + results_dir_stream + 'NearestLake_' + stream + '_streams.csv'


elif scale == 'Nicole': #  high resolution data provided by Nicole
    connection_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                     + results_dir_stream + '/PureConnection/'
    results_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/Hr_Nicole/withoutGL/' \
                  + results_dir_stream + 'NearestLake_' + stream + '_streams.csv'

pairs = {}
for filename in os.listdir(connection_dir):
    filepath = connection_dir + filename
    print(filename)
    with open(filepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['stream(COMID)'] not in pairs:
                pairs[row['stream(COMID)']] = [row['LENGTHKM'], row['Lake(lagosid)']]
            if float(row['LENGTHKM']) < float(pairs[row['stream(COMID)']][0]):
                pairs[row['stream(COMID)']] = [row['LENGTHKM'], row['Lake(lagosid)']]

with open(results_dir, 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['stream(COMID)', 'Lake(lagosid)', 'LENGTHKM'])
    for i in pairs:
        writer.writerow([i, pairs[i][1], pairs[i][0]])

debug_point = 1


