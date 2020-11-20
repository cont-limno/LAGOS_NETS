import os
import copy
import csv
import collections
import random
from traversal_dij.functions import data, names
from traversal_dij.functions import RemoveCircle


scale = 'Nicole' #  medium resolution or other resolution (Nicole, Hr)

Type = 'Downstream'
lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
           FCodeName, Flowdir, FromName, ToName, LAGOSName, \
           results_dir, OrderPath, PairPath = names.get_names_lake_order(scale, Type)


lagosid, lagos_filename, weight, FCode = data.load(scale, lagosIDpath, weightPath, weightName,
               FCodePath, FCodeName, ID_flow, LAGOSName)

StreamOrder = data.getOrder(OrderPath)
# check if stream order has non-positive value
for i in StreamOrder:
    if int(StreamOrder[i]) <= 0 and i in lagosid:
        print("%s order number is %s"%(i, StreamOrder[i]))

lagosid_set = set(lagosid.values())
lagosid_setLAGOSNAME = set()
for i in lagosid_set:
    lagosid_setLAGOSNAME.add(i + LAGOSName)

pairs_dup = data.constructPairsUS(Flowdir, FromName, ToName, weight,
                                  FCode, Type, lagosid,
                                  LAGOSName, PairPath, scale)

# remove pairs self connection
pairs = {}
for i in pairs_dup:
    pairs[i] = {}
    for nei in pairs_dup[i]:
        if nei != i:
            pairs[i][nei] = pairs_dup[i][nei]

print('Adjacency construction finished')

vertices = set(pairs.keys())
LakeOrder = collections.defaultdict(list)
lakes_lakes = {} # lakes with lakes neighbors
for i in vertices:
    if i in lagosid_setLAGOSNAME:
        order_i = -1
        for neighbor in pairs[i]:
            if neighbor in StreamOrder:
                order_i = max(order_i, int(StreamOrder[neighbor]))
            if neighbor in lagosid_setLAGOSNAME: # neighbor is a lake
                if i not in lakes_lakes:
                    lakes_lakes[i] = []
                if neighbor not in lakes_lakes[i]:
                    lakes_lakes[i].append(neighbor)
        LakeOrder[i].append(order_i)  # since one lake may have multiple lines

LakeOrderMax = {}
for i in LakeOrder:
    LakeOrderMax[i] = max(LakeOrder[i])
del LakeOrder


# lake_lake connection circulus exist, random remove them
lake_lake_no_circle, circles = RemoveCircle.RmCircle(lakes_lakes).remove()
print(len(lake_lake_no_circle), len(circles))
del lakes_lakes
lakes_lakes_list = list(set(lake_lake_no_circle.keys()))


while lakes_lakes_list:
    random.shuffle(lakes_lakes_list) #  if not shuffle, loop will not finish
    i = lakes_lakes_list.pop()
    print(i)
    FLAG = 0
    for nei in lake_lake_no_circle[i]:
        # if the neighbor is only connected with streams or it's neighbors have been checked
        if nei not in lakes_lakes_list:
            LakeOrderMax[i] = max(LakeOrderMax[i], LakeOrderMax[nei])
        else:
            FLAG = 1
        if FLAG == 1:
            lakes_lakes_list.append(i)

### starting deal with 2 special cases
# if lake is a terminal lake, lake order should be max of inflowing streams
lakes_outflowing = {i: False for i in lagosid_setLAGOSNAME}
lakes_terminal_set = set()
for i in lakes_outflowing:
    if i in pairs and len(pairs[i]) > 0:
        lakes_outflowing[i] = True
for i in lakes_outflowing:
    if not lakes_outflowing[i]:
        lakes_terminal_set.add(i)

LakeOrderSecond = collections.defaultdict(list)
for i in vertices:
    for neighbor in pairs[i]:
        if neighbor in lakes_terminal_set:
            if i in StreamOrder:
                LakeOrderSecond[neighbor].append(int(StreamOrder[i]))  # since one lake may have multiple lines

LakeOrderMaxSecond = {}
for i in LakeOrderSecond:
    LakeOrderMaxSecond[i] = max(LakeOrderSecond[i])
# merge LakeOrderMaxSecond with LakeOrder
for i in LakeOrderMaxSecond:
    if LakeOrderMax[i] != -1:
        print("Error")
    LakeOrderMax[i] = LakeOrderMaxSecond[i]


# if lake has no inflowing streams, lake order should be 0
lakes_inflowing = {i: False for i in lagosid_setLAGOSNAME}
for i in vertices:
    for neighbor in pairs[i]:
        if neighbor in lakes_inflowing: # if it has inflowing streams
            lakes_inflowing[neighbor] = True
for i in lakes_inflowing:
    if not lakes_inflowing[i]:
        LakeOrderMax[i] = 0


# with open(results_dir + 'LakeOrder.csv', 'w') as csv_file:
#     writer = csv.writer(csv_file)
#     writer.writerow(['lagosid', 'LakeOrder'])
#     for key in LakeOrderMax:
#         if key not in lagosid_setLAGOSNAME:
#             print('Error!')
#         key_re = key.replace(LAGOSName, '')
#         writer.writerow([key_re, LakeOrderMax[key]])

