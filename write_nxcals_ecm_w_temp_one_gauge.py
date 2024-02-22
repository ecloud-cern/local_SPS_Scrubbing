import pytimber
import numpy as np
import datetime
import datascout as ds
import os
import os.path
import sys
sys.path.append('..')
import overview_manager
import pendulum
import shutil
from scrubbing_info import s_year, s_month, s_day, s_hour, s_minute
from scrubbing_info import USERS
from scrubbing_info import local_directory

db = pytimber.LoggingDB()

# directory to save data
output_directory = f"{local_directory}/data/ECM_nxcals/"

overwrite = False

#devices = ['BESCLD-VECM11733', 'BESCLD-VECM11737', 'BESCLD-VECM11738', 'BESCLD-VECM11754']
devices = ['BESCLD-VECM11732', 'BESCLD-VECM11737', 'BESCLD-VECM11738', 'BESCLD-VECM11754']
#devices = ['BESCLD-VECM11737']
# devices = ['BESCLD-VECM11732']

# USERS = ['MD5', 'LHC25NS', 'LHCMD3']
# USERS = ['MD5']

# variables to be saved in parquets from ECM data
value_list = ['acqDesc', 'acqMsg', 'acqTime', 'cycleLength', 'measStamp', 'nbOfChannels', 'nbOfMeas', 'propType', 'sem2DRaw', 'semRawSum', 'superCycleNb', 'totalGain']

# create/link to one h5 file device and user.
overview_files = {}
overview_names = {}

variables = []
for dev in devices:
    overview_name = f'{local_directory}/data/Overviews_nxcals/ecm_overview_{dev}.h5'
    overview_names[dev] = overview_name
    for value in value_list:
        variables.append(f'{dev}:Acquisition:{value}')


variables.append('SPS.TGM:USER')

# download from date of last recorded file until today (one hour ago)
list_of_days = os.listdir(output_directory + '/' + devices[0])
if list_of_days == []:
    #start of scrubbing date
    last_hour_date = datetime.datetime(s_year, s_month, s_day, s_hour, s_minute) #.strftime("%Y-%m-%d %H:%M:%S")  
else:
    last_day = max(list_of_days)
    list_of_files = os.listdir(output_directory + '/' + devices[0] + '/' +last_day)
    # list_of_files might be empty!!
    if len(list_of_files) != 0:
        last_file = max(list_of_files)
        print('last_file:')
        print(last_file)
        year, month, day, hour, minute, second, microsecond, parquet = last_file.split('.')
        last_hour_date = datetime.datetime(int(year), int(month), int(day), int(hour))# - datetime.timedelta(hours=1)
    else:
        year, month, day = last_day.split('-')
        last_hour_date = datetime.datetime(int(year), int(month), int(day), 0)
        print(f'no files in {last_day}, setting last hour date to: {last_hour_date}')

previous_date = last_hour_date
# previous_date = datetime.datetime(2023, 3, 24, 18, 30)

# can only download 12 hours at a time to avoid memory overflow
highest_date = previous_date + datetime.timedelta(hours = 25)
# datetime_now = datetime.datetime.now()
# datetime_now = datetime.datetime(2023, 4, 18, 16, 00)
datetime_now = datetime.datetime(s_year, s_month, s_day, s_hour + 1, s_minute )

if datetime_now > highest_date:
    datetime_now = highest_date
    print(f'Time interval too large to download, instead downloading data from {previous_date} to {highest_date}.') 
    print("Run script again until today's date and time are reached.")
else:
    print(f'Downloading most recent data, datetime_now = {datetime_now}')

# for the days to dowload data from make hour time ranges for the full time.
time_ranges_to_download = []
while previous_date + datetime.timedelta(hours=1) < datetime_now:
    time_ranges_to_download.append((
        previous_date.strftime("%Y-%m-%d %H:%M:%S"),
        (previous_date + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    ))
    previous_date += datetime.timedelta(hours=1)

time_ranges_to_download.append((
    previous_date.strftime("%Y-%m-%d %H:%M:%S"),
    (datetime_now - datetime.timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
))
print('Downloading in time ranges:')
print(time_ranges_to_download)

temp_folders = {}
temp_overview_files = {}
temp_overview_names = {}

for dev in devices:
    temp_folder = f'{output_directory}temp2/{dev}/'
    temp_folders[dev] = temp_folder
    os.makedirs(temp_folder)
    temp_overview_name = f'{temp_folder}ecm_overview_{dev}.h5'
    temp_overview_names[dev] = temp_overview_name
    # if the file does not exist it is no problem, just get a message that it cannot be copied
    os.system(f'cp {overview_names[dev]} {temp_overview_name}')
    temp_overview_files[dev] = overview_manager.open_file(temp_overview_name)

curr_day = ''
for t_start, t_end in time_ranges_to_download:
    print(f'Fetching data between {t_start} and {t_end} ...')

    data = db.get(variables, t_start, t_end)
    cycleStamp_list = data[variables[-1]][0]
    print(f'len(cycleStamp_list) = {len(cycleStamp_list)}')
    if curr_day != t_start[0:10]:
        curr_day = t_start[0:10]

    for dev in devices:
        print(f'device {dev}')
        temp_folder = temp_folders[dev]

        if not os.path.exists(temp_folder+'/'+curr_day):
            os.mkdir(temp_folder+'/'+curr_day)
            if not os.path.exists(output_directory+ '/' + dev + '/'+curr_day):
                os.mkdir(output_directory+ '/' + dev + '/'+curr_day)
        zero_counter = 0
        for cycle_index, cycleStamp_s in enumerate(cycleStamp_list):
            SPS_user = data['SPS.TGM:USER'][1][cycle_index]
            if SPS_user == 'ZERO':
                zero_counter += 1

            cycle_index = cycle_index - zero_counter
            
            cycleStamp_time = pendulum.from_timestamp(cycleStamp_s, tz="Europe/Paris")    

            print(f'user {SPS_user} at {cycleStamp_time}')
            if SPS_user not in USERS:
                continue
            cycleStamp_ns = int(cycleStamp_s * 1.e9)
    
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
    
            full_dictionary = {}
            device = f'{dev}:Acquisition:'
            full_dictionary[device] = {}

            ### build header
            header = {
                'acqStamp' : 0,
                'cycleStamp' : cycleStamp_ns,
                'isFirstUpdate' : False,
                'isImmediateUpdate' : False,
                'selector': 'SPS:USER:' + SPS_user,
                'setStamp': 0
                }
            full_dictionary[device]['header'] = header

            ### build value
            value = {}
            for val_name in value_list:
                temp_val = data[f'{device}{val_name}'][1]
                #if len(temp_val) == len(cycleStamp_list):
                if cycle_index < len(temp_val):
                    #value[val_name] = temp_val[cycle_index]
                    if val_name == 'measStamp':
                        value[val_name] = np.float_(temp_val[cycle_index])
                    else:
                        value[val_name] = temp_val[cycle_index]
                else:
                    value[val_name] = 0.
                    print(f'error1: strange length of data, len(temp_val) = {len(temp_val)}, len(cycleStamp_list) = {len(cycleStamp_list)} and cycle_index = {cycle_index}')

            stamp_device = f'{int(cycleStamp_s)}/{device}/'
      
            full_dictionary[device]['value'] = value
        
            # Write the dict to temp parquet file
            ds.dict_to_parquet(full_dictionary, temp_final_path)
               
            if value['nbOfMeas'] == '': # empty measuements are not saved in h5
                print(f'nbOfMeas empty for cyclestamp {cycleStamp_time}')
                continue
            n_meas_eff = int(value['nbOfMeas'])
            signal = value['sem2DRaw'].astype(float) / value['totalGain']

            signal_sum = np.nansum(signal,axis=0)

            injection_ecloud_index = np.argmin(abs(value['measStamp'] - 500.0))
            injection_ecloud = signal_sum[:n_meas_eff][injection_ecloud_index]
            max_ecloud = np.max(signal_sum[:n_meas_eff])
        
            overview_manager.write_value(cycleStamp_ns, f'{stamp_device}cycleStamp_ns', temp_overview_files[dev])
            overview_manager.write_value(injection_ecloud, f'{stamp_device}injection_ecloud', temp_overview_files[dev])
            overview_manager.write_value(max_ecloud, f'{stamp_device}max_ecloud', temp_overview_files[dev])
            overview_manager.write_value(str(SPS_user), f'{stamp_device}user', temp_overview_files[dev])


for h5file in overview_files.keys():
    overview_manager.close_file(temp_overview_files[h5file])

# Move the temps to the proper folders
for dev in devices:
    shutil.move(temp_overview_names[dev],overview_names[dev])
    if not os.path.exists(output_directory+'/' + dev):
        os.mkdir(output_directory+'/' + dev)
    for curr_day in os.listdir(temp_folders[dev]):
        if os.path.isdir(temp_folders[dev] + curr_day):
            print(f'transferring temp for = {curr_day}')
            if not os.path.exists(output_directory+'/' + dev + '/'+curr_day):
                os.mkdir(output_directory+'/' + dev + '/'+curr_day)
            for parq in os.listdir(f'{temp_folders[dev]}{curr_day}/'):
                shutil.move(f'{temp_folders[dev]}{curr_day}/{parq}', f'{output_directory}/{dev}/{curr_day}/{parq}')

# Remove the temp folder
os.system('rm -r ' + f'{output_directory}temp2')
