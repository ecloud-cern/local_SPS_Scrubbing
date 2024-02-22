import h5py
import numpy
import os


def open_file(fname='overview.h5'):
    if os.path.exists(fname):
        rw_opts = 'a'
    else:
        rw_opts = 'w'
    
    h5_file = h5py.File(fname, rw_opts)
    return h5_file
    
    
def write_value(value, value_path, h5_file):
    stampname, device, value_name =  value_path.split('/')
    if stampname in h5_file.keys():
        grp = h5_file[stampname]
    else:
        grp = h5_file.create_group(stampname)
    
    if device in grp.keys():
        grp2 = grp[device]
    else:
        grp2 = grp.create_group(device)
    
    if value_name in grp2:
        data = grp2[value_name]
        data[...] = value
    else:
        grp2[value_name] = value

def close_file(h5_file):
    h5_file.close()

# print(h5py.File('overview.h5','r')['2021/mydevice/max_intensity'][()])
# write_value(3.e-5, '2021/mydevice/max_intensity')
# print(h5py.File('overview.h5','r')['2021/mydevice/max_intensity'][()])
#stampname/device/value

