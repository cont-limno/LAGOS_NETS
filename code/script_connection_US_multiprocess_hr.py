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


def core(vertices_partbyloc, location, vertices, pairs, results_dir, LAGOSName, weightName, savepath):
    # find connections
    if savepath:
        print('%s has %d lakes. Paths will be saved.' % (location, len(vertices_partbyloc)))
    else:
        print('%s has %d lakes. Paths will not be saved.' % (location, len(vertices_partbyloc)))

    dij = DijkstraAlg.Dijkstra(pairs, vertices, LAGOSName, savepath)
    import time
    start_time = time.time()
    connections_all = []
    for i in vertices_partbyloc:
        conn = dij.traversal(i)
        connections_all.extend(conn)
    ### FROM TO path_length, path, path_depth 0101 unidirection

    with open(results_dir + '/PureConnection/' + location + '_connection.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['Lake1', 'Lake2', weightName, 'UpstreamsLength',
             'DpstreamsLength'])
        for j in connections_all:
            writer.writerow([j[0], j[1], j[2], j[3], j[4]])
    print("Traversal finished for %s.\t "
          "Total lakes number %d \t"
          "Time cost%.4f minutes" %
          (location, len(vertices_partbyloc),
           (time.time() - start_time) * 1.0 / 60))
    if savepath:
        with open(results_dir + '/Connection/' + location + '_connection.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Lake1', 'Lake2', weightName, 'UpstreamsLength',
                             'DpstreamsLength', 'Path'])
            for j in connections_all:
                writer.writerow([j[0], j[1], j[2], j[3], j[4], j[5]])

        print("Connection with paths file write finished for %s.\t " % location)
    return None


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-scale', metavar='S', type=str, nargs='+',
                        default=['Hr'],
                        help='Resolution: Hr or Med.')
    parser.add_argument('-stype', metavar='S', type=str, nargs='+',
                        default=['All'],
                        help='Streams will be included: All, Upstream, Downstream')
    parser.add_argument('-savepath', metavar='Bool', type=bool, nargs='+',
                        default=[False],
                        help='If save path between lakes.')
    return parser.parse_args()


def get_args():
    args = parse_args()
    scale = args.scale[0]
    stype = args.stype[0]
    savepath = args.savepath[0]
    print('scale:%s,\tstype:%s\tsavepath:%s'%
          (scale, stype, savepath))
    return scale, stype, savepath


def chunks_dict(data, SIZE=1):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


if __name__ == "__main__":
    scale, stype, savepath = get_args()
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
    # partition lakes by its location
    vertices_partbyloc = {} # lakes partitioned by location (filename)
    location_set = set()
    for i in vertices:
        i_re = i.replace(LAGOSName, '')
        if i in lagosid_setLAGOSNAME:
            name_i_re = lagos_filename[i_re]
            if name_i_re not in location_set:
                vertices_partbyloc[name_i_re] = [i]
                location_set.add(name_i_re)
            else:
                vertices_partbyloc[name_i_re].append(i)

    del weight
    del FCode
    del lagosid
    del lagos_filename
    del lagosid_setLAGOSNAME

    # delete the HUCs that have already finished from vertices_partbyloc
    for file in os.listdir(results_dir + '/PureConnection/'):
        name = file.replace('_connection.csv', '')
        del vertices_partbyloc[name]

    if not os.path.isdir(results_dir + '/Connection/'):
        os.makedirs(results_dir + '/Connection/')
    if not os.path.isdir(results_dir + '/PureConnection/'):
        os.makedirs(results_dir + '/PureConnection/')
    for chunked_vertices in chunks_dict(vertices_partbyloc, SIZE=15):
        processes = []
        for location in chunked_vertices:

            if not os.path.isfile(results_dir + '/PureConnection/' + location + '_connection.csv'):

                p = mp.Process(target=core, args=(
                   vertices_partbyloc[location], location, vertices, pairs,
                results_dir, LAGOSName, weightName, savepath,))
                processes.append(p)
                p.start()

        for process in processes:
            process.join()

                # core(vertices_partbyloc[location], location, vertices, pairs,
                #          results_dir, LAGOSName, weightName)


