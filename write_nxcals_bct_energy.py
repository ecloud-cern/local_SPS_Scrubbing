import pytimber
import numpy as np
import datetime
import datascout as ds
import os
import os.path
import overview_manager
import pendulum
from scrubbing_info import s_year, s_month, s_day, s_hour, s_minute
from scrubbing_info import USERS
from scrubbing_info import local_directory

db = pytimber.LoggingDB()


output_directory = f"{local_directory}/data/BCT_nxcals"
overwrite = False

device = 'SPS.BCTDC.51456/Acquisition'   #old device 51454
device_to_overview = 'SPS.BCTDC.51456'

# download from date of last recorded file until today (one hour ago)
list_of_days = os.listdir(output_directory + '/' + device_to_overview)
if list_of_days == []:
    # start of scrubbing date
    last_hour_date = datetime.datetime(s_year, s_month, s_day, s_hour, s_minute) #.strftime("%Y-%m-%d %H:%M:%S")  
else:
    last_day = max(list_of_days)
    list_of_files = os.listdir(output_directory + '/' + device_to_overview + '/' +last_day)
    # list_of_files might be empty!!
    last_file = max(list_of_files)
    print('last_file:')
    print(last_file)
    year, month, day, hour, minute, second, microsecond, parquet = last_file.split('.')
    last_hour_date = datetime.datetime(int(year), int(month), int(day), int(hour)) # - datetime.timedelta(hours=1)

previous_date = last_hour_date
#previous_date = datetime.datetime(2023, 3, 24, 18, 30)
#previous_date = datetime.datetime(2023, 3, 25, 10, 00)

datetime_now = datetime.datetime.now()
datetime_now = datetime.datetime(s_year, s_month, s_day, s_hour + 1, s_minute )
#datetime_now = datetime.datetime(2023, 4, 25, 17, 5)

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

###############

overview_name = f'{local_directory}/data/Overviews_nxcals/BCT_overview_' + device_to_overview + '.h5'


variables = [device_to_overview + ':Acquisition:totalIntensity',  
             device_to_overview + ':Acquisition:totalIntensity_unitExponent',
             device_to_overview + ':Acquisition:measStamp',
             device_to_overview + ':Acquisition:measStamp_unitExponent',
             'SPSQC:DUMP_ENERGY',
             'SPS.TGM:USER']

temp_folder = f'{local_directory}/data/BCT_nxcals/temp/' + device_to_overview + '/'
temp_overview_name = temp_folder + '/BCT_overview_' + device_to_overview + '.h5'

os.makedirs(temp_folder)
os.system('cp ' + overview_name + ' ' + temp_overview_name)

# Open the temp overview file
temp_overview_file = overview_manager.open_file(temp_overview_name)

curr_day = ''
for t_start, t_end in time_ranges_to_download:
    print(f'Fetching data between {t_start} and {t_end} ...')
    if curr_day != t_start[0:10]:
        curr_day = t_start[0:10]
        os.mkdir(temp_folder+'/'+curr_day)
        if not os.path.exists(output_directory + '/' + device_to_overview + '/' + curr_day):
            os.mkdir(output_directory + '/' + device_to_overview + '/' + curr_day)

    data = db.get(variables, t_start, t_end)
    
    cycleStamp_list = data["SPS.TGM:USER"][0]
    if len(data["SPS.TGM:USER"][0]) != len(data[variables[0]][0]):
        print(f"len of SPS.TGM:USER = {len(data['SPS.TGM:USER'][0])}")
        print(f"len of BCT = {len(data[variables[0]][0])}")
        print("Warning: variables are not aligned. Number of cycles not equal for all timber variables")
    for cycle_index, cycleStamp_s in enumerate(cycleStamp_list):
        SPS_user = data['SPS.TGM:USER'][1][cycle_index]
        if SPS_user not in USERS:
            continue
        if cycle_index >= len(data[variables[0]][0]):
            break
        if data['SPS.TGM:USER'][0][cycle_index] != data[variables[0]][0][cycle_index]:
            print(data['SPS.TGM:USER'][1][cycle_index])
            print(data['SPS.TGM:USER'][0][cycle_index] - data[variables[0]][0][cycle_index])
            print("WARNING: SKIPPED a cycle")
            continue
            #assert data['SPS.TGM:USER'][0][cycle_index] == data[variables[0]][0][cycle_index]
        cycleStamp_ns = int(cycleStamp_s * 1.e9)
        
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
    
        full_dictionary = {}
        full_dictionary[device] = {}

        ### build header
        header = {
                  'acqStamp' : 0,
                  'cycleStamp' : cycleStamp_ns,
                  'isFirstUpdate' : False,
                  'isImmediateUpdate' : False,
                  'selector': 'SPS:USER:' + SPS_user,
                  'setStamp': 0,
                 }
        full_dictionary[device]['header'] = header
        
        ### build value
        totalIntensity = np.float_(data[device_to_overview + ':Acquisition:totalIntensity'][1][cycle_index])
        totalIntensity_unitExponent = data[device_to_overview + ':Acquisition:totalIntensity_unitExponent'][1][cycle_index]
        measStamp = np.float_(data[device_to_overview + ':Acquisition:measStamp'][1][cycle_index])
        measStamp_unitExponent = data[device_to_overview + ':Acquisition:measStamp_unitExponent'][1][cycle_index]
        acqTime = ''

        time_axis = measStamp * 10**measStamp_unitExponent
        intensity_axis = totalIntensity * 10**totalIntensity_unitExponent

        # define injection intensity at 25ms of BCT timestamps
        injection_intensity_index = np.argmin(abs(time_axis - 25e-3))         
        injected_intensity_protons = intensity_axis[injection_intensity_index]
        maximum_intensity_protons = np.max(intensity_axis)
        delta_time = np.diff(time_axis)[0]
        integrated_intensity_protons_seconds = np.sum(intensity_axis)*delta_time

        # get dump energy
        energy_index_array = np.where(data['SPSQC:DUMP_ENERGY'][0] == cycleStamp_s)[0]
        if len(energy_index_array) == 0:
            energy_at_dump = 0.
        elif len(energy_index_array) == 1:
            energy_at_dump = data['SPSQC:DUMP_ENERGY'][1][energy_index_array[0]]
        else:
            print('Multiple values at time stamp!!')


        # check exponents
        value = {
                 'totalIntensity' : totalIntensity,
                 'totalIntensity_unitExponent' : totalIntensity_unitExponent,
                 'measStamp' : measStamp,
                 'measStamp_unitExponent' : measStamp_unitExponent,
                 'injectionTime_wrt_cycleStamp_s' : 1.015,
                 'acqTime' : acqTime,
                 'injected_intensity_protons' : injected_intensity_protons,
                 'maximum_intensity_protons' : maximum_intensity_protons,
                 'integrated_intensity_protons_seconds' : integrated_intensity_protons_seconds,
                 'energy_at_dump' : energy_at_dump,
                }
        stamp_device = f'{int(cycleStamp_s)}/{device_to_overview}/'

        full_dictionary[device]['value'] = value
        
        # Write the dict to temp parquet file
        ds.dict_to_parquet(full_dictionary, temp_final_path)

        # Modify the temp overview file  
        overview_manager.write_value(injected_intensity_protons, stamp_device + 'injected_intensity_protons', temp_overview_file)
        overview_manager.write_value(maximum_intensity_protons, stamp_device + 'maximum_intensity_protons', temp_overview_file)
        overview_manager.write_value(integrated_intensity_protons_seconds, stamp_device + 'integrated_intensity_protons_seconds', temp_overview_file)
        overview_manager.write_value(cycleStamp_ns, stamp_device + 'cycleStamp_ns', temp_overview_file)
        overview_manager.write_value(energy_at_dump, stamp_device + 'energy_at_dump', temp_overview_file)

# Close the overview file
overview_manager.close_file(temp_overview_file)       

# Move the temps to the proper folders
os.system('mv ' + temp_overview_name + ' ' + overview_name)

if not os.path.exists(output_directory+'/' + device_to_overview):
    os.mkdir(output_directory+'/' + device_to_overview)

for curr_day in os.listdir(temp_folder):
    print(curr_day)

    if not os.path.exists(output_directory+'/' + device_to_overview + '/'+curr_day):
        os.mkdir(output_directory+'/' + device_to_overview + '/'+curr_day)

    os.system('mv ' + temp_folder + curr_day + '/*.parquet ' + output_directory+'/' + device_to_overview + '/'+ curr_day)

# Remove the temp folder
os.system('rm -r ' + temp_folder)

