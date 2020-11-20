import os
import csv
from itertools import islice
import argparse
import multiprocessing as mp
from traversal_dij.functions import data, names
from traversal_dij.functions import NearestDam

"""
Use dijkstra algorithm to find the nearest upstream and downstream dam
Tested!
"""


def core(vertices_partbyloc, location, lakes_50m_dam, dam_on_lake, COMID_damID,
         vertices, pairs, LAGOSName, resultpath):
    # find connections
    if savepath:
        print('%s has %d lakes. Paths will be saved.' % (location, len(vertices_partbyloc)))
    else:
        print('%s has %d lakes. Paths will not be saved.' % (location, len(vertices_partbyloc)))
    nearest_dist = [] # store the distance and the DAMID
    dij = NearestDam.Dijkstra(pairs, vertices, COMID_damID, LAGOSName)
    for i in vertices_partbyloc: # start from a lake
        FLAG = 0
        if i in dam_on_lake: # if dam is on the lake
            FLAG = 1
            for dj in dam_on_lake[i]:
                nearest_dist.append([i.replace(LAGOSName, ''), 0, dj])
        elif i in lakes_50m_dam: # if dam is not on the lake
                                 # but on a stream near to the lake
            for dj in lakes_50m_dam[i]:
                if dj in pairs[i]: # if the stream is in the correct direction,
                                                 # distanct is defined as 0
                    FLAG = 2
                    for dam in COMID_damID[dj]:
                        nearest_dist.append([i.replace(LAGOSName, ''), 0,
                                                      dam])

        if FLAG == 0:
            dist = dij.traversal(i)
            if dist is not None:
                for dam in COMID_damID[dist[1]]:
                    nearest_dist.append([i.replace(LAGOSName, ''), dist[0], dam])
    print('Finished')
    with open(resultpath, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['lagoslakeid', 'DamDistance', 'DAMID'])
        for j in nearest_dist:
            writer.writerow([j[0], j[1], j[2]])
    return None


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-scale', metavar='S', type=str, nargs='+',
                        default=['Med'],
                        help='Resolution: Hr or Med.')
    parser.add_argument('-stype', metavar='S', type=str, nargs='+',
                        default=['Downstream'],
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
    return scale, stype, savepath


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunks_dict(data, SIZE=1):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


if __name__ == "__main__":
    scale, stype, savepath = get_args()
    print('scale:%s,\tstype:%s\tsavepath:%s' %
          (scale, stype, savepath))
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
    del lagos_filename
    del lagosid_setLAGOSNAME

    lakes_50m_dam, dam_on_lake_upstream, dam_on_lake_downstream, \
             COMID_damID = data.getDam(LAGOSName, lagosid)
    if stype == 'Downstream':
        dam_on_lake = dam_on_lake_downstream
    elif stype == 'Upstream':
        dam_on_lake = dam_on_lake_upstream

    results_dir = results_dir + '/Dam/Nearest/'
    if not os.path.isdir(results_dir ):
        os.makedirs(results_dir)

    for chunked_vertices in chunks_dict(vertices_partbyloc, SIZE=10):
        processes = []
        for location in chunked_vertices:
            resultpath = results_dir + location + '_NearestDam.csv'
            if not os.path.isfile(resultpath):
                p = mp.Process(target=core, args=(
                   vertices_partbyloc[location], location, lakes_50m_dam,
                   dam_on_lake, COMID_damID, vertices, pairs, LAGOSName, resultpath,))
                processes.append(p)
                p.start()

        for process in processes:
            process.join()

                # core(vertices_partbyloc[location], location, vertices, pairs,
                #          results_dir, LAGOSName, weightName)


