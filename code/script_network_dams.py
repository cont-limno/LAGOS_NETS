import os
import csv
from itertools import islice
import argparse
from traversal_dij.functions import data, names

"""
Use DFS search to find the total dam in a chain
Tested!
"""


def DFS(source, dam_on_lake, COMID_dam, pairs, LAGOSName):
    lake_set = set()
    dam_set = set()
    visited = set()
    stack = []
    stack.append(source)
    visited.add(source)
    while stack:
        curr = stack.pop()
        if curr in COMID_dam:
            dam_set = dam_set.union(COMID_dam[curr])
        if curr in dam_on_lake:
            dam_set = dam_set.union(dam_on_lake[curr])
        if curr.find(LAGOSName) != -1:
            lake_set.add(curr)
        for nei in pairs[curr]:
            if nei not in visited:
                stack.append(nei)
                visited.add(nei)
    return len(dam_set), dam_set, lake_set


def main(IDsLakes, dam_on_lake, COMID_dam, pairs, LAGOSName, resultpath):
    # find connections
    dam_num = []
    for i in IDsLakes:
        # start from a random lake in the chain and do DFS search
        dam_num_i, dam_set_i, lake_set_i = DFS(IDsLakes[i][0], dam_on_lake, COMID_dam, pairs, LAGOSName)
        dam_num.append([i, dam_num_i, dam_set_i])
        # check if all lakes in the chain has been visited
        for lake in IDsLakes[i]:
            if lake not in lake_set_i:
                print('Error!')
    print('Finished')
    with open(resultpath, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['ChainID', 'DamNum'])
        for j in dam_num:
            if j[1] != 0:
                writer.writerow([j[0], j[1]])
    return None


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-scale', metavar='S', type=str, nargs='+',
                        default=['Med'],
                        help='Resolution: Hr or Med.')
    return parser.parse_args()


def get_args():
    args = parse_args()
    scale = args.scale[0]
    print('scale:%s,\t'%
          (scale))
    return scale


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunks_dict(data, SIZE=1):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


if __name__ == "__main__":
    scale = get_args()
    stype = 'All'
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

    del weight
    del FCode
    del lagos_filename
    del lagosid_setLAGOSNAME

    lakes_50m_dam, dam_on_lake_upstream, dam_on_lake_downstream, \
                    COMID_damID = data.getDam(LAGOSName, lagosid)

    # merge the dam_on_lake_upstream and dam_on_lake_downstream
    dam_on_lake = dam_on_lake_upstream
    for i in dam_on_lake_downstream:
        if i not in dam_on_lake:
            dam_on_lake[i] = dam_on_lake_downstream[i]
        else:
            dam_on_lake[i] = dam_on_lake[i].union(dam_on_lake_downstream[i])

    chainIDfilepath = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/'\
                      + scale +'/withoutGL/' \
                      + stype + '/LakeChain/MedLakeChainIDs.csv'
    IDsLakes = {}
    with open(chainIDfilepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['chain_id'] not in IDsLakes:
                IDsLakes[row['chain_id']] = []
            IDsLakes[row['chain_id']].append(row['lagoslakeid']+LAGOSName)

    resultpath = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/results/'\
                      + scale +'/withoutGL/' \
                      + stype + '/LakeChain_TotalDams.csv'
    main(IDsLakes, dam_on_lake, COMID_damID, pairs, LAGOSName, resultpath)




