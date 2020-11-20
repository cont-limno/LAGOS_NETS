import os
import csv
from traversal_dij.functions import GL_info
from traversal_dij.functions import names, data

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def pairs_clean(scale, lagosid, FCode, FromName, ToName, data_dir, result_dir):

    FCodeGreatLakes = GL_info.FCodeGL(scale)
    GL_PID_list = GL_info.GL(scale)
    coastlinePID = data.getWBPID_coastline(scale, lagosid)
    removed_PID = set('0')
    removed_PID = removed_PID.union(GL_PID_list)
    removed_PID = removed_PID.union(coastlinePID)

    for filename in os.listdir(data_dir):
        saved_name = result_dir + filename
        with open(saved_name, mode='w') as save_csv_file:
            writer = csv.writer(save_csv_file)
            writer.writerow([FromName, ToName])
            with open(data_dir + filename, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    rowFrom = row[FromName]
                    rowTo = row[ToName]
                    if rowFrom not in FCode: FCode[rowFrom] = 'NoFCode'
                    if rowTo not in FCode: FCode[rowTo] = 'NoFCode'

                    if rowFrom not in removed_PID and rowTo not in removed_PID \
                            and (FCode[rowFrom] not in FCodeGreatLakes) \
                            and (FCode[rowTo] not in FCodeGreatLakes):

                        writer.writerow([rowFrom, rowTo])


scale = 'Med'
stype = 'Downstream'
lagosIDpath, ID_flow, weightPath, weightName, FCodePath, \
   FCodeName, Flowdir, FromName, ToName, LAGOSName, \
             results_dir, PairPath = names.get_names(scale, stype)

lagosid, lagos_filename, weight, FCode = data.load(scale,
                                    lagosIDpath, weightPath, weightName,
                                     FCodePath, FCodeName, ID_flow, LAGOSName)

saved_dir = '/home/wangqi19/GPU1data/LakeNetwork/preprocess/intermediate_tables/Med/cleaned_flows/'
flow_path = '/home/wangqi19/GPU1data/LakeNetwork/raw_data/Med/NHDmed_flowlines_all-2/flow/'

if not os.path.isdir(saved_dir):
    os.makedirs(saved_dir)

pairs_clean(scale, lagosid, FCode, FromName, ToName, flow_path, saved_dir)


