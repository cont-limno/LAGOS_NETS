import os
import csv
from itertools import islice
import argparse
import multiprocessing as mp
from traversal_dij.functions import data, names

"""
Use DFS search to find the total up/down stream dam
Tested!
"""


def DFS(source, dam_on_lake, COMID_dam, pairs, LAGOSName):
    dam_set = set()
    # if a dam is on lake
    if source in dam_on_lake:
        dam_num = len(dam_on_lake[source])
        dam_set = dam_set.union(dam_on_lake[source])
    else:
        dam_num = 0
    visited = set()
    stack = []
    stack.append(source)
    visited.add(source)
    while stack:
        curr = stack.pop()
        if curr in COMID_dam:
            dam_num += len(COMID_dam[curr])
            dam_set = dam_set.union(COMID_dam[curr])
        for nei in pairs[curr]:
            # if nei is not a lake and has not been visited before
            if nei not in visited and nei.find(LAGOSName) == -1:
                stack.append(nei)
                visited.add(nei)
    return dam_num, dam_set


def main(vertices_partbyloc, dam_on_lake, COMID_dam, pairs, LAGOSName, resultpath):
    # find connections
    dam_num = []
    for i in vertices_partbyloc:
        dam_num_i, dam_set_i = DFS(i, dam_on_lake, COMID_dam, pairs, LAGOSName)
        if dam_num_i != len(dam_set_i):
            print('Error!')
        dam_num.append([i.replace(LAGOSName, ''), dam_num_i, dam_set_i])

    print('Finished')
    with open(resultpath, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['lagoslakeid', 'DamNum', 'DamCOMID'])
        for j in dam_num:
            if j[1] != 0:
                writer.writerow([j[0], j[1], j[2]])
    return None


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-scale', metavar='S', type=str, nargs='+',
                        default=['Med'],
                        help='Resolution: Hr or Med.')
    parser.add_argument('-stype', metavar='S', type=str, nargs='+',
                        default=['Upstream'],
                        help='Streams will be included: All, Upstream, Downstream')
    return parser.parse_args()


def get_args():
    args = parse_args()
    scale = args.scale[0]
    stype = args.stype[0]
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
    print('scale:%s,\tstype:%s\t' %
          (scale, stype))
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

    vertices = set(list(pairs.keys()))  # only need the key since the pairs' keys contains all
                                        # the from and to lakes in the table.

    # partition lakes by its location
    vertices_partbyloc = {} # lakes partitioned by location (filename)
    location_set = set()
    for i in vertices:
        if i in lagosid_setLAGOSNAME:
            i_re = i.replace(LAGOSName, '')
            name_i_re = lagos_filename[i_re]
            if name_i_re not in location_set:
                vertices_partbyloc[name_i_re] = [i]
                location_set.add(name_i_re)
            else:
                vertices_partbyloc[name_i_re].append(i)

    lakes_50m_dam, dam_on_lake_upstream, dam_on_lake_downstream, \
                     COMID_damID = data.getDam(LAGOSName, lagosid)

    del weight
    del FCode
    del lagos_filename
    del vertices

    if stype == 'Downstream':
        dam_on_lake = dam_on_lake_downstream
    elif stype == 'Upstream':
        dam_on_lake = dam_on_lake_upstream

    results_dir = results_dir + '/Dam/Total' + stype + 'Dam/'
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

    for chunked_vertices in chunks_dict(vertices_partbyloc, SIZE=10):
        processes = []
        for location in chunked_vertices:
            resultpath = results_dir + location + '_' + stype + 'Dam.csv'
            if not os.path.isfile(resultpath):
                p = mp.Process(target=main, args=(
                   vertices_partbyloc[location], dam_on_lake, COMID_damID, pairs,
                   LAGOSName, resultpath,))

                processes.append(p)
                p.start()

        for process in processes:
            process.join()

                # core(vertices_partbyloc[location], location, vertices, pairs,
                #          results_dir, LAGOSName, weightName)


