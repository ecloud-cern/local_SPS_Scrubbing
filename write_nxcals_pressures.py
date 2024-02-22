import pytimber
print('imported PyTimber')
import numpy as np
import datetime
import datascout as ds
import os.path
import overview_manager
import sys
import matplotlib.pyplot as plt
import pendulum

from scrubbing_info import s_year, s_month, s_day, s_hour, s_minute
from scrubbing_info import USERS
from scrubbing_info import local_directory

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--gauge', nargs='?', default=None, type=str)
args = parser.parse_args()


db = pytimber.LoggingDB()
bct_device = 'SPS.BCTDC.51456' #51454
bct_directory = f'{local_directory}/data/BCT_nxcals/' + bct_device + '/'
output_directory = f'{local_directory}/data/Pressures_nxcals/'
overwrite = True

# Kickers:
# gauge = 'MKDVB.51698:PRESSURE'
# gauge = 'MKDHB.51757:PRESSURE'
gauge = 'MKDHA.51751:PRESSURE'
# gauge = 'MKDHA.51754:PRESSURE'
# gauge = 'MKE.41637:PRESSURE'
# gauge = 'MKP.11955:PRESSURE'
# gauge = 'MKP.11955:TEMPERATURE.1'

# gauge = 'VACCMW.VGHB_11931.PR' # MKP area
# gauge = 'VACCMW.VGHB_11936.PR' # MKP area
# gauge = 'VACCMW.VGHB_11952.PR' # MKP area
# gauge = 'VACCMW.VGHB_11959.PR' # MKP area

# gauge = 'VACCMW.VGHB_41631.PR' # MKE4 area
# gauge = 'VACCMW.VGHB_41634.PR' # MKE4 area
# gauge = 'VACCMW.VGHB_41637.PR' # MKE4 area
# gauge = 'VACCMW.VGHB_41651.PR' # MKE4 area

# Vented/new equipment:
# gauge = 'VACCMW.VGHB_11733.PR' # ECM
# gauge = 'VACCMW.VGHB_11754.PR' # ECM

# gauge = 'VACCMW.VGHB_61770.PR' # MST
# gauge = 'VACCMW.VGHB_61773.PR' # MST
# gauge = 'VACCMW.VGHB_61776.PR' # MST
# gauge = 'VACCMW.VGHB_61793.PR' # MST

# gauge = VACCMW.'VGHB_61837.PR' # MSE
# gauge = 'VACCMW.VGHB_61851.PR' # MSE
# gauge = 'VACCMW.VGHB_61871.PR' # MSE

# gauge = 'VACCMW.VGHB_32308.PR' # BI ++
# gauge = 'VACCMW.VGHB_32708.PR' # BI ++
# gauge = 'VACCMW.VGHB_32970.PR' # BI ++
# gauge = 'VACCMW.VGHB_33105.PR' # BI ++

# gauge = 'VACCMW.VGHB_41693.PR' # FWS ++
# gauge = 'VACCMW.VGHB_41651.PR' # MKE4
# gauge = 'VACCMW.VGHB_51340.PR' # FWS ++
# gauge = 'VACCMW.VGHB_51480.PR' # FWS ++
# gauge = 'VACCMW.VGHB_51540.PR' # FWS ++

# gauge = 'VPIA_22520.PR'
# gauge = 'VPIA_61904.PR' # Ion pumps
# gauge = 'VACCMW.VGHB_62060.PR' # Ion pumps
# gauge = 'VPIA_62060.PR' # Ion pumps
# gauge = 'VACCMW.VGHB_62260.PR' # Ion pumps
# gauge = 'VPIA_62480.PR' # Ion pumps
gauge = 'VACCMW.VGHB_62940.PR' # Ion pumps
# gauge = 'VACCMW.VGHB_63160.PR' # Ion pumps
# gauge = 'VACCMW.VGHB_10060.PR' # Ion pumps

# gauge = 'VACCMW.VGHB_11692.PR' # TT10 access
# gauge = 'VACCMW.VGHB_2181?.PR' # TT20 access
# gauge = 'VACCMW.VGHB_61891.PR' # TT60 access

# gauge = 'SPS.ABWLM:BUNCH_PEAK_MEAN'  # ???

# Vented/new magnets:

# Other locations:
# gauge = 'VACCMW.VGHB_10660.PR'
# gauge = 'VACCMW.VGHB_20660.PR'
# gauge = 'VACCMW.VGHB_40060.PR'
# gauge = 'VACCMW.VGHB_43160.PR'

if args.gauge is not None:
    gauge = args.gauge

print(f'downloading data from gauge {gauge}')

prr
output_directory = output_directory + gauge + '/'
overview_name = f'{local_directory}/data/Overviews_nxcals/Pressures/' + gauge + '_overview.h5'

# Temp filenames
temp_parent = f'{local_directory}/data/Pressures_nxcals/temps_pressure/'
temp_folder = temp_parent + gauge + '/'
temp_overview_name = temp_folder + gauge + '_overview.h5'

if not os.path.exists(temp_parent):
    os.mkdir(temp_parent)

os.mkdir(temp_folder)
os.system('cp ' + overview_name + ' ' + temp_overview_name)

if os.path.exists(temp_overview_name):
    print('it copied')
else:
    print('it did not copy')

# Open the temp overview file
temp_overview_file = overview_manager.open_file(temp_overview_name)

list_of_gauges = [ gauge ] # don't put more gauges here!!!!!!!!!!!!!!!!!!!

for device in list_of_gauges:
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)

# download from date of last recorded file until today (one hour ago)
list_of_days = os.listdir(output_directory)

if list_of_days == []:
    last_hour_date = datetime.datetime(s_year, s_month, s_day, s_hour, s_minute) #.strftime("%Y-%m-%d %H:%M:%S")
else:
    last_day = max(list_of_days)
    list_of_files = os.listdir(output_directory + '/' + last_day)
    print(f'last day = {last_day}')
    last_file = max(list_of_files)
    year, month, day, hour, minute, second, microsecond, parquet = last_file.split('.')
    last_hour_date = datetime.datetime(int(year), int(month), int(day), int(hour)) # - datetime.timedelta(hours=1)

previous_date = last_hour_date
# previous_date = datetime.datetime(2023, 4, 20, 8, 00)

datetime_now = datetime.datetime.now()
datetime_now = datetime.datetime(s_year, s_month, s_day, s_hour+1, s_minute)

two_minutes = datetime.timedelta(minutes=2)

time_ranges_to_download_datetime = []

while previous_date + datetime.timedelta(hours=1) < datetime_now:
    time_ranges_to_download_datetime.append((
        (previous_date - two_minutes), 
        (previous_date + datetime.timedelta(hours=1) + two_minutes)
    ))
    previous_date += datetime.timedelta(hours=1)

time_ranges_to_download_datetime.append(( (previous_date - two_minutes), 
                                          (datetime_now  + two_minutes) ))

print('Downloading in time ranges:')
for time_range in time_ranges_to_download_datetime:
    print((time_range[0].strftime("%Y-%m-%d %H:%M:%S"), time_range[1].strftime("%Y-%m-%d %H:%M:%S")))

for t_start_datetime, t_end_datetime in time_ranges_to_download_datetime:
    t_start = t_start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    t_end = t_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    print(f'Fetching data between {t_start} and {t_end} ...')
    nxcals_data = db.get(list_of_gauges, t_start, t_end)

    ### resample data to every second.
    data = {}
    valid_device = {}
    for device in nxcals_data.keys():
       old_time = nxcals_data[device][0]
       old_pressure = nxcals_data[device][1]
       
       if len(old_time) < 2:
           valid_device[device] = False
           continue
       else:
           valid_device[device] = True

       new_time = np.arange(old_time[0], old_time[-1] + 0.5)
       new_pressure = np.zeros_like(new_time)

       ii = 0
       for kk in range(len(old_time)-1):
           while old_time[kk+1] - 0.5 > new_time[ii]:
               new_pressure[ii] = old_pressure[kk]
               ii += 1
       new_pressure[-1] = old_pressure[-1]

       data[device] = (new_time, new_pressure)

    list_bct_day_dirs = os.listdir(bct_directory)
    list_of_all_bct_files = []
    for bct_day_directory in list_bct_day_dirs:
        if bct_day_directory != 'temp':
            for bct_file in os.listdir(bct_directory + '/' + bct_day_directory):
                list_of_all_bct_files.append(bct_file)

    list_of_bct_files = []
    for bct_file in list_of_all_bct_files:
        year, month, day, hour, minute, second, microsecond, parquet = bct_file.split('.')
        file_date = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        if file_date > t_start_datetime and file_date < t_end_datetime:
            list_of_bct_files.append(bct_file)

    curr_day_dots = ''
    for bct_file in list_of_bct_files: 
    
        if bct_file[0:10] != curr_day_dots:
            curr_day_dots = bct_file[0:10]
            curr_day = bct_file[0:10].replace('.', '-')
            if not os.path.exists(temp_folder + curr_day):
                os.mkdir(temp_folder + curr_day)
        
        input_file = bct_directory + curr_day + '/' + bct_file
        
        try:
            bct = ds.parquet_to_dict(input_file)
        except:
            print(f'no parquet from bct for {input_file}')
            continue
        

        cycleStamp_ns = bct[f'{bct_device}/Acquisition']['header']['cycleStamp'] 
        # we only take user and cyclestamp from BCT in this scrip so it does not matter which btc device is used

        cycleStamp_s = cycleStamp_ns * 1.e-9
        
        cycleStamp_time = pendulum.from_timestamp(cycleStamp_s, tz="Europe/Paris")
        
        filename = cycleStamp_time.strftime("%Y.%m.%d.%H.%M.%S.%f") + '.parquet'

        temp_final_path = temp_folder + curr_day + '/' + filename 
        if os.path.exists(temp_final_path):
            if overwrite:
                print(f'{filename} exists, overwriting...')
            else:
                print(f'{filename} exists, not overwriting...')
                continue
        else:
                print(f'Creating {filename}...')
        
        user = bct[f'{bct_device}/Acquisition']['header']['selector']
        # print(f'user = {user}')

        t_start_mask = cycleStamp_s
        if user == "SPS:USER:MD5":
            cycle_length = 25 ## how much should we put?
        elif user == "SPS:USER:LHC25NS":
            cycle_length = 23 ## how much should we put?
        elif user == "SPS:USER:LHCMD3":
            cycle_length = 28 ## how much should we put?
        elif user == "SPS:USER:LHC2":
            cycle_length = 23 ## how much should we put?
        else:
            cycle_length = 8.2 ## 7.2 for the length of the cycle, plus 1 second
        t_end_mask = cycleStamp_s + cycle_length
        
        full_dictionary = {}
        for device in list_of_gauges:
            if not valid_device[device]:
                continue
            full_dictionary[device] = {}
        
            ### build header
            header = {
                      'acqStamp' : 0,
                      'cycleStamp' : cycleStamp_ns,
                      'isFirstUpdate' : False,
                      'isImmediateUpdate' : False,
                      'selector': user,
                      'setStamp': 0,
                     }
            full_dictionary[device]['header'] = header
            
            ### build value
            mask = np.logical_and(t_start_mask < data[device][0], data[device][0] < t_end_mask)
            time_axis = data[device][0][mask] - cycleStamp_s # [s]
            pressure_axis = data[device][1][mask] # mbar
            
            if len(pressure_axis) == 0:
                print(f'device {device} is empty')
                max_pressure_mbar = -1
            else:
                max_pressure_mbar = np.max(pressure_axis)
            
            # check exponents
            value = {
                     'time_s': time_axis,
                     'pressure_mbar' : pressure_axis,
                     'max_pressure_mbar' : max_pressure_mbar,
                    }
            stamp_device = f'{int(cycleStamp_s)}/{device}/'
            overview_manager.write_value(max_pressure_mbar, stamp_device + 'max_pressure_mbar', temp_overview_file)
            overview_manager.write_value(user, stamp_device + 'user', temp_overview_file)

            full_dictionary[device]['value'] = value
        
        ds.dict_to_parquet(full_dictionary, temp_final_path)

# Close the overview file
overview_manager.close_file(temp_overview_file)

# Move the temps to the proper folders
os.system('mv ' + temp_overview_name + ' ' + overview_name)

for day_folder in os.listdir(temp_folder):
    if not os.path.exists(output_directory + day_folder):
                os.mkdir(output_directory + day_folder)

    os.system('mv ' + temp_folder + day_folder + '/* ' + output_directory + day_folder)

# Remove the temp folder
os.system('rm -r ' + temp_folder)
