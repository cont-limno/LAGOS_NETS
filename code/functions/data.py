import csv
import os
import warnings
from traversal_dij.functions import GL_info


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def matchID(scale, filepath):
    """ match lagos IDs with the flow IDs"""
    lakes = {}  # a dictionary contains all the Permanent_Identifier for lakes
    lagosid = {}
    if scale == 'Med':
        if not os.path.isfile(filepath):

            with open('/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/LAGOS_NHDPlusv2_Crosswalk.csv',
                      mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    lakes[row['nhdplusv2_comid']] = row[
                        'lagoslakeid']  ### this nhdplusv2_comid is also WBArea_COMID

            dir = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/info/'  # a dictionary to store all flow pairs
            for filename in os.listdir(dir):
                save_name = os.path.splitext(filename)[0]
                save_name = save_name.replace('Flowlineinfo', '')
                with open(dir + filename, mode='r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        if (row['WBAREACOMI'] in lakes) \
                                and (row['COMID'] not in lagosid):
                            lagosid[row['COMID']] = [lakes[row['WBAREACOMI']],
                                                     save_name]
            with open(filepath, 'w') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['COMID', 'lagoslakeid', 'filename'])
                for key, value in lagosid.items():
                    writer.writerow([key, value[0], value[1]])
        lagosid_read = {}
        lagos_filename_read = {}
        with open(filepath, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                lagosid_read[row['COMID']] = row['lagoslakeid']
                lagos_filename_read[row['lagoslakeid']] = row['filename'] # saved as lagosid: filename
        print('Match ID finished')

    else:
        if not os.path.isfile(filepath):
            with open('/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/LAGOS_US_All_Lakes_1ha_v05.csv', mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    lakes[row['Permanent_Identifier']] = row['lagoslakeid']  # this PID is also WBPID

            dir = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/NHDFlowline/'# a dictionary to store ID mapping

            for filename in os.listdir(dir):
                save_name = os.path.splitext(filename)[0]
                save_name = save_name.replace('NHDFlowline_', '')
                with open(dir + filename, mode='r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        if (row['WBArea_Permanent_Identifier'] in lakes)\
                                and (row['Permanent_Identifier'] not in lagosid):
                            lagosid[row['Permanent_Identifier']] = [lakes[row['WBArea_Permanent_Identifier']], save_name]

            with open(filepath, 'w') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Permanent_Identifier', 'lagoslakeid', 'filename'])
                for key, value in lagosid.items():
                   writer.writerow([key, value[0], value[1]])
        lagosid_read = {}
        lagos_filename_read = {}
        with open(filepath, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                lagosid_read[row['Permanent_Identifier']] = row['lagoslakeid']
                lagos_filename_read[row['lagoslakeid']] = row['filename']
        print('Match ID finished')
    return lagosid_read, lagos_filename_read


def getWeight(scale, weightPath,
                   weightName, ID_flow, lagosid, LAGOSName, FCode):
    """
    scale: Med or others
    weight_name:  '../raw_data/NHDmed_flowlines_all-2/info/'  for med
    ID_flow: the ID name for the flow
    weight_name: the column name for weight, i.e., 'LENGTHKM' for med
    """
    if scale == 'Med':
        #  get the weight
        dir_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/info/'
    else:
        dir_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/NHDFlowline2/'
    weight = {}
    if not os.path.isfile(weightPath):
        for filename in os.listdir(dir_info):
            with open(dir_info + filename, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    # if the flow is a virtual flow for a lake,
                    # the length is set to be 0.
                    if row[ID_flow] in lagosid:
                        weight[row[ID_flow]] = 0.0
                    else:
                        weight[row[ID_flow]] = row[weightName]

        with open(weightPath, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([ID_flow, weightName])
            for key in weight:
                writer.writerow([key, weight[key]])

    weight_read = {}
    with open(weightPath, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            rowIDFlow = lagosid[row[ID_flow]] + LAGOSName if row[ID_flow] in lagosid \
                                else row[ID_flow]
            weight_read[rowIDFlow] = row[weightName]
    print('Load weights finished.')
    return weight_read

def getFCode(scale, FCodePath,
                   FCodeName, ID_flow, lagosid, LAGOSName):
    """
    scale: Med or others
    weight_name:  '../raw_data/NHDmed_flowlines_all-2/info/'  for med
    ID_flow: the ID name for the flow
    weight_name: the column name for weight, i.e., 'LENGTHKM' for med
    """
    if scale == 'Med':
        #  get the weight
        dir_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/info/'
    else:
        dir_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/NHDFlowline2/'
    FCode = {}
    if not os.path.isfile(FCodePath):
        for filename in os.listdir(dir_info):
            with open(dir_info + filename, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    FCode[row[ID_flow]] = row[FCodeName]

        with open(FCodePath, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([ID_flow, FCodeName])
            for key in FCode:
                writer.writerow([key, FCode[key]])

    FCode_read = {}
    with open(FCodePath, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            rowIDFlow = lagosid[row[ID_flow]] + LAGOSName if row[ID_flow] in lagosid \
                                      else row[ID_flow]
            FCode_read[rowIDFlow] = row[FCodeName]
    print('Load FCodes finished.')
    return FCode_read

def load(scale, lagosIDpath, weightPath, weightName,
               FCodePath, FCodeName, ID_flow, LAGOSName):
    """map the indices of data files.
       lagosIDpath: the file to store matched ids,
                 if exist, directly load, else match them and save the file
       ID_flow: the ID name in the flow data,
                i.e., COMID for med data,
                      Permanent_Identifier for others
       ID_lagos: 'lagoslakeid'
       weightName: column name of the weight,
                    i.e., 'LENGTHKM' if Med and use length as weight
    """

    lagosid, lagos_filename = matchID(scale, lagosIDpath)


    FCode = getFCode(scale, FCodePath,
                   FCodeName, ID_flow, lagosid, LAGOSName)

    weight = getWeight(scale, weightPath,
                       weightName, ID_flow, lagosid, LAGOSName, FCode)

    return lagosid, lagos_filename, weight, FCode

def getOrder(OrderPath):
    """
    Get the stream orders. Only for med resolution.
    OrderPath: file to store all the stream orders
    """
    dir_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/order/'
    Order = {}
    if not os.path.isfile(OrderPath):
        for filename in os.listdir(dir_info):
            with open(dir_info + filename, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    Order[row['ComID']] = row['StreamOrde']

        with open(OrderPath, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['COMID', 'StreamOrder'])
            for key in Order:
                writer.writerow([key, Order[key]])

    Order_read = {}
    with open(OrderPath, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            Order_read[row['COMID']] = row['StreamOrder']
    print('Load order finished.')
    return Order_read

def getWBPID_coastline(scale, lagosid):
    if scale == 'Med' or scale == 'Test':
        return set()
    else:
        dir = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/NHDFlowline/'
        coastlineWBPID = set()
        count = 0
        with open('/home/wangqi19/GPU1data/LakeNetwork/raw_data/Nicole/coastline_wbID.csv', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                coastlineWBPID.add(row['WBArea_PID'])
                count += 1
        print('Total coastlineWBPID %d.' % count)

        coastlinePID = set()
        count = 0
        for filename in os.listdir(dir):
            with open(dir + filename, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    if row['WBArea_Permanent_Identifier'] in coastlineWBPID \
                            and row['Permanent_Identifier'] not in lagosid:
                        coastlinePID.add(row['Permanent_Identifier'])
                        count += 1
        savepath = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/intermediate_tables/Nicole/coastlinePID.csv'
        with open(savepath, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key in coastlinePID:
                writer.writerow([key])
        print('Total estuaries %d.' % count)
        return coastlinePID



def pairsCore(filename_list, FromName, ToName, weight,
                   FCode, Type, lagosid, LAGOSName, scale):
    pairs = {}
    count_all = 0
    count_none = 0

    FCodeGreatLakes = GL_info.FCodeGL(scale)
    GL_PID_list = GL_info.GL(scale)
    coastlinePID = getWBPID_coastline(scale, lagosid)

    removed_PID = set('0')
    removed_PID = removed_PID.union(GL_PID_list)
    removed_PID = removed_PID.union(coastlinePID)

    for filename in filename_list:
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                #  add 'LAGOS' in case lagosid has conflict with the stream IDs
                rowFrom = lagosid[row[FromName]] + LAGOSName if row[FromName] in lagosid \
                            else row[FromName]
                rowTo = lagosid[row[ToName]] + LAGOSName if row[ToName] in lagosid \
                                    else row[ToName]
                if rowFrom not in FCode: FCode[rowFrom] = 'NoFCode'
                if rowTo not in FCode: FCode[rowTo] = 'NoFCode'

                if rowFrom not in removed_PID and rowTo not in removed_PID \
                        and (FCode[rowFrom] not in FCodeGreatLakes) \
                        and (FCode[rowTo] not in FCodeGreatLakes):

                    if rowFrom not in pairs:
                        pairs[rowFrom] = {}
                    if rowTo not in pairs:
                        pairs[rowTo] = {}

                    #  downstreams (the original from-to table)
                    if Type == 'Downstream' or Type == 'All':
                        if rowTo in weight:
                            if isfloat(weight[rowTo]):
                                pairs[rowFrom][rowTo] = [weight[rowTo],
                                                         'Downstream']
                                count_all += 1

                        else:
                            count_none += 1

                    #  upstreams (virtual links)
                    if Type == 'Upstream' or Type == 'All':
                        if rowFrom in weight:
                            if isfloat(weight[rowFrom]):
                                pairs[rowTo][rowFrom] = [weight[rowFrom],
                                                         'Upstream']
                                count_all += 1
                        else:
                            count_none += 1

    print('Total connection number is %d. \n'
          'The number of connections do not have weight is %d.'
          % (count_all, count_none))
    return pairs


def constructPairsHuc(filename, FromName, ToName, weight,
                   FCode, Type, lagosid, LAGOSName, scale):
    """
    Load the files and construct adjacency list
    :param filename_all: file path
    :param FromName: the column name for the from flow
    :param ToName: the column name for the to flow
    :return: pairs dictionary
    """
    pairs = pairsCore([filename], FromName, ToName, weight,
                   FCode, Type, lagosid, LAGOSName, scale)

    return pairs


def constructPairsUS(Flowdir, FromName, ToName, weight,
                   FCode, Type, lagosid, LAGOSName, PairPath, scale):
    """
    Load the files and construct adjacency list
    :param filename_all: file path
    :param FromName: the column name for the from flow
    :param ToName: the column name for the to flow
    :return: pairs dictionary
    """
    if not os.path.isfile(PairPath):
        filepath_list = [Flowdir + x for x in os.listdir(Flowdir)]
        pairs = pairsCore(filepath_list, FromName, ToName, weight,
                   FCode, Type, lagosid, LAGOSName, scale)

        with open(PairPath, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['From', 'To', 'Weight', 'Direction'])
            for lakefrom in pairs:
                lakesto = pairs[lakefrom]
                for lake in lakesto:
                    content = lakesto[lake]
                    writer.writerow([lakefrom, lake, content[0], content[1]])

    pairs_read = {}
    with open(PairPath, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            rowFrom = row['From']
            rowTo = row['To']
            if rowFrom not in pairs_read:
                pairs_read[rowFrom] = {}
            if rowTo not in pairs_read:
                pairs_read[rowTo] = {}
            pairs_read[rowFrom][rowTo] = [float(row['Weight']), row['Direction']]
    return pairs_read


def getDam(LAGOSName, lagosid):
    """
    Get the stream orders. Only for med resolution.
    OrderPath: file to store all the stream orders
    Note: one COMID or lagosID may have multiple dams on it (multiple DAMIDs)
          ond DAMID only havs one lagosID or COMID
    """
    dam_info = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NABDv2_dams.csv'
    dams_on_lake_file = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/dams_on_lakes_direction.csv'
    dams_direction = {} # a dict for dam directions if the dam is on a lake
    with open(dams_on_lake_file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            dams_direction[row['DAMID']] = row['direction']

    lakes_50m_dam = {} # lakes that have a dam within 50 meters (store the COMIDs)
    dam_on_lake_upstream = {} # lagosid which contain upstream dams (store the DAMIDS)
    dam_on_lake_downstream = {}  # lagosid which contain downstream dams (store the DAMIDS)
    COMID_damID = {} # streams that have a dam include those within 50m of lakes (store the DAMIDS)
    with open(dam_info, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['lagoslakei'] != '0':
                if row['COMID'] in lagosid and lagosid[row['COMID']] == row['lagoslakei']: # if dam on lakes
                    if dams_direction[row['DAMID']] == 'up': # if upstream dam
                        if row['lagoslakei'] + LAGOSName not in dam_on_lake_upstream:
                            dam_on_lake_upstream[row['lagoslakei'] + LAGOSName] = set()
                        dam_on_lake_upstream[row['lagoslakei'] + LAGOSName].add(row['DAMID'])

                    elif dams_direction[row['DAMID']] == 'down' or dams_direction[row['DAMID']] == 'NA': # if downstream dam
                        if row['lagoslakei'] + LAGOSName not in dam_on_lake_downstream:
                            dam_on_lake_downstream[row['lagoslakei'] + LAGOSName] = set()
                        dam_on_lake_downstream[row['lagoslakei'] + LAGOSName].add(row['DAMID'])

                    else:
                        print('Dam direction has string issue.')
                else: # if dam is within 50m of a lake
                    if row['lagoslakei']+LAGOSName not in lakes_50m_dam:
                        lakes_50m_dam[row['lagoslakei'] + LAGOSName] = set() # may have multiple <50m dams for this lake
                    if row['COMID'] not in lakes_50m_dam[row['lagoslakei']+LAGOSName]:
                        lakes_50m_dam[row['lagoslakei']+LAGOSName].add(row['COMID'])

                    if row['COMID'] not in COMID_damID: # update COMID_damID since the
                                                             # dam is on a stream
                        COMID_damID[row['COMID']] = set()
                    COMID_damID[row['COMID']].add(row['DAMID'])
            else:
                if row['COMID'] not in COMID_damID:
                    COMID_damID[row['COMID']] = set()
                COMID_damID[row['COMID']].add(row['DAMID'])
    return lakes_50m_dam, dam_on_lake_upstream, dam_on_lake_downstream, COMID_damID


if __name__ == "__main__":
    pass
