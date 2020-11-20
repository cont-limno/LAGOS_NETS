import os
import csv
from itertools import islice
import warnings
import argparse
import multiprocessing as mp
from traversal_dij.functions import data, names
from traversal_dij.functions import DijkstraAlg

"""
Cannot directly store the lakes with the lagosid since lagosid 
       and PID or COMID may have conflicts.
"""


def core(vertices_start, vertices, pairs, results_dir, LAGOSName, weightName):
    # find connections
    dij = DijkstraAlg.Dijkstra(pairs, vertices, LAGOSName, savepath=False)
    import time
    start_time = time.time()
    connections_all = []
    for i in vertices_start:
        conn = dij.traversal(i)
        connections_all.extend(conn)
    with open(results_dir + '/PureConnection/streams/connection.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['stream(COMID)', 'Lake(lagosid)', weightName, 'UpstreamsLength',
             'DownstreamsLength'])
        for j in connections_all:
            writer.writerow([j[0], j[1], j[2], j[3], j[4]])
    print("Traversal finished."
          "Total lakes number %d \t"
          "Time cost%.4f minutes" %
          (len(vertices_start),
           (time.time() - start_time) * 1.0 / 60))
    return None


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-scale', metavar='S', type=str, nargs='+',
                        default=['Med'],
                        help='Resolution: Hr or Med.')
    parser.add_argument('-stype', metavar='S', type=str, nargs='+',
                        default=['All'],
                        help='Streams will be included: All, Upstream, Downstream')
    return parser.parse_args()


def get_args():
    args = parse_args()
    scale = args.scale[0]
    stype = args.stype[0]
    print('scale:%s,\tstype:%s'%
          (scale, stype))
    return scale, stype


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunks_dict(data, SIZE=1):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


if __name__ == "__main__":
    scale, stype = get_args()
    lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
    FCodeName, Flowdir, FromName, ToName, LAGOSName, \
                 results_dir, PairPath = names.get_names(scale, stype)


    lagosid, lagos_filename, weight, FCode = data.load(scale,
                                    lagosIDpath, weightPath, weightName,
                                     FCodePath, FCodeName, ID_flow, LAGOSName)

    lagosid_set = set(lagosid.values())
    lagosid_setLAGOSNAME = set()
    for i in lagosid_set:
        lagosid_setLAGOSNAME.add(i+LAGOSName)


    pairs = data.constructPairsUS(Flowdir, FromName, ToName, weight,
                                    FCode, stype, lagosid,
                                    LAGOSName, PairPath, scale)

    vertices = set(list(pairs.keys())) # only need the key since the pairs' keys contains all
                                       # the from and to lakes in the table.
    # load start stream COMID
    stream_path = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/stream.csv'
    vertices_start = []
    with open(stream_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['COMID'] in pairs:
                vertices_start.append(row['COMID'])
            else:
                print(row['COMID'])

    del weight
    del FCode
    del lagosid
    del lagos_filename
    del lagosid_setLAGOSNAME

    if not os.path.isdir(results_dir + '/Connection/'):
        os.makedirs(results_dir + '/Connection/')
    if not os.path.isdir(results_dir + '/PureConnection/streams/'):
        os.makedirs(results_dir + '/PureConnection/streams/')
    core(vertices_start, vertices, pairs, results_dir, LAGOSName, weightName)


