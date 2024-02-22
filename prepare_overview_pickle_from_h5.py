import h5py
import numpy as np
import matplotlib.pyplot as plt
import pickle
import datetime
import sys
from scrubbing_info import s_year, s_month, s_day, s_hour, s_minute
from scrubbing_info import USERS
from scrubbing_info import local_directory

# Kickers:
# gauge_name = 'MKDVB.51698:PRESSURE'
# gauge_name = 'MKDHB.51757:PRESSURE'
# gauge_name = 'MKDHA.51751:PRESSURE'
# gauge_name = 'MKDHA.51754:PRESSURE'
# gauge_name = 'MKE.41637:PRESSURE'
gauge_name = 'MKP.11955:PRESSURE'
# gauge_name = 'MKP.11955:TEMPERATURE.1'

# gauge_name = 'VGHB_11931.PR' # MKP area
# gauge_name = 'VGHB_11936.PR' # MKP area
# gauge_name = 'VGHB_11952.PR' # MKP area
# gauge_name = 'VGHB_11959.PR' # MKP area

# gauge_name = 'VGHB_41631.PR' # MKE4 area
# gauge_name = 'VGHB_41634.PR' # MKE4 area
# gauge_name = 'VGHB_41637.PR' # MKE4 area
# gauge_name = 'VGHB_41651.PR' # MKE4 area

# Vented/new equipment:
# gauge_name = 'VGHB_11733.PR' # ECM
# gauge_name = 'VGHB_11754.PR' # ECM

# gauge_name = 'VGHB_61770.PR' # MST
# gauge_name = 'VGHB_61773.PR' # MST
# gauge_name = 'VGHB_61776.PR' # MST
# gauge_name = 'VGHB_61793.PR' # MST

# gauge_name = 'VGHB_61837.PR' # MSE
# gauge_name = 'VGHB_61851.PR' # MSE
# gauge_name = 'VGHB_61871.PR' # MSE

# gauge_name = 'VGHB_32308.PR' # BI ++
# gauge_name = 'VGHB_32708.PR' # BI ++
# gauge_name = 'VGHB_32970.PR' # BI ++
# gauge_name = 'VGHB_33105.PR' # BI ++

# gauge_name = 'VGHB_41693.PR' # FWS ++
# gauge_name = 'VACCMW.VGHB_41651.PR' # MKE4
# gauge_name = 'VGHB_51340.PR' # FWS ++
# gauge_name = 'VGHB_51480.PR' # FWS ++
# gauge_name = 'VGHB_51540.PR' # FWS ++

# gauge_name = 'VPIA_22520.PR'
# gauge_name = 'VPIA_61904.PR' # Ion pumps
# gauge_name = 'VACCMW.VGHB_62060.PR' # Ion pumps
# gauge_name = 'VPIA_62060.PR' # Ion pumps
# gauge_name = 'VACCMW.VGHB_62260.PR' # Ion pumps
# gauge_name = 'VPIA_62480.PR' # Ion pumps
# gauge_name = 'VGHB_62940.PR' # Ion pumps
# gauge_name = 'VGHB_63160.PR' # Ion pumps
# gauge_name = 'VGHB_10060.PR' # Ion pumps

# gauge_name = 'VACCMW.VGHB_11692.PR' # TT10 access
# gauge_name = 'VACCMW.VGHB_2181?.PR' # TT20 access
# gauge_name = 'VACCMW.VGHB_61891.PR' # TT60 access

# Vented/new magnets:

# Other locations:
# gauge_name = 'VACCMW.VGHB_10660.PR'
# gauge_name = 'VACCMW.VGHB_20660.PR'
# gauge_name = 'VACCMW.VGHB_40060.PR'
# gauge_name = 'VACCMW.VGHB_43160.PR'

print('Variable: ' + gauge_name)

start_date = datetime.datetime(s_year, s_month, s_day, s_hour, s_minute) 
# start_date = datetime.datetime(2023, 4, 20, 8, 00) # year, month, day, hour (-2 to get it in utc time maybe??)
start_cycle = datetime.datetime.timestamp(start_date) # convert to uct

data_folder = f'{local_directory}/data/Overviews_nxcals/'

def get_var(cycleStamp_list_str, h5_file, value_path):
    var = []
    for cycleStamp in cycleStamp_list_str:
       if value_path in h5_file:
           vv = h5_file[value_path][()]
       else:
           vv = -1 
       vv.append(var) 
    return

bct_device = 'SPS.BCTDC.51456' #new 51456   old 51454

BCT_h5 = h5py.File(data_folder + 'BCT_overview_' + bct_device +'.h5', 'r')

cycleStamps_list_str = list(BCT_h5.keys())

def max_pressure_list(gauge_name, cycleStamps_list_str_p, user, start_cycle):
    # convert from date to cyclestamp
    max_list = []
    cycleStamps_list_user = []
    VGHB_h5 = h5py.File(data_folder + 'Pressures/'+ gauge_name + '_overview.h5', 'r')
    max_cycleStamp = max(list(VGHB_h5.keys()))
    print(f'max_cycleStamp = {max_cycleStamp}')

    for cycleStamp in cycleStamps_list_str_p:
        if cycleStamp > max_cycleStamp:
            break
        if cycleStamp < str(start_cycle):
            continue

        if cycleStamp in VGHB_h5.keys():
            try:
                temp_user = VGHB_h5[cycleStamp][gauge_name]['user'][()].decode()
            except:
                continue
        else:
            continue

        if(temp_user == user):
            max_list.append(VGHB_h5[cycleStamp][gauge_name]['max_pressure_mbar'][()])
            cycleStamps_list_user.append(cycleStamp)

    return max_list, cycleStamps_list_user

    
cycleStamps_list = list(map(int, cycleStamps_list_str))


# user_list = ['SPS:USER:MD5', 'SPS:USER:LHC25NS', 'SPS:USER:LHCMD3', 'SPS:USER:LHC2']
user_list = [f'SPS:USER:{us}' for us in USERS]
# user_list = ['SPS:USER:MD5', 'SPS:USER:LHC25NS']

for user in user_list:
    print(f'Making pickels for user {user}')
    mydict = {}
    max_pressure, cycleStamps_list_user = max_pressure_list(gauge_name, cycleStamps_list_str, user, start_cycle)
    mydict['cycleStamps'] = list(map(int, cycleStamps_list_user))
    mydict['integrated_intensity'] = [BCT_h5[cycleStamp][f'{bct_device}']['integrated_intensity_protons_seconds'][()] for cycleStamp in cycleStamps_list_user]
    mydict['max_pressure_' + gauge_name] = max_pressure
    mydict['max_intensity'] = [BCT_h5[cycleStamp][f'{bct_device}']['maximum_intensity_protons'][()] for cycleStamp in cycleStamps_list_user]

    pickle.dump(mydict, open('overview_pickles/overview_from_h5_' + gauge_name + '_' + user + '_' + bct_device + '_' + str(start_date)[0:10] + '.pkl', 'wb'))
 
