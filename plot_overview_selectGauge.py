import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import cm
import pickle
import datetime
import sys

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'arial'

# Kickers:
# gauge_name = 'MKDVB.51698:PRESSURE'
# gauge_name = 'MKDHB.51757:PRESSURE'
# gauge_name = 'MKDHA.51751:PRESSURE'
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
# gauge_name = 'VACCMW.VGHB_41651.PR' # FWS ++
# gauge_name = 'VGHB_51340.PR' # FWS ++
# gauge_name = 'VGHB_51480.PR' # FWS ++
# gauge_name = 'VGHB_51540.PR' # FWS ++

# gauge_name = 'VACCMW.VGHB_62060.PR' # Ion pumps
# gauge_name = 'VPIA_62060.PR' # Ion pumps
# gauge = 'VACCMW.VGHB_62260.PR' # Ion pumps
# gauge_name = 'VPIA_62480.PR' # Ion pumps
# gauge_name = 'VGHB_62940.PR' # Ion pumps
# gauge_name = 'VGHB_63160.PR' # Ion pumps
# gauge_name = 'VGHB_10060.PR' # Ion pumps

# gauge_name = 'VGHB_11692.PR' # TT10 access
# gauge_name = 'VGHB_2181?.PR' # TT20 access
# gauge_name = 'VGHB_61891.PR' # TT60 access

# Vented/new magnets:

# Other locations:
# gauge_name = 'VACCMW.VGHB_10660.PR'
# gauge_name = 'VACCMW.VGHB_20660.PR'
# gauge_name = 'VACCMW.VGHB_40060.PR'
# gauge_name = 'VACCMW.VGHB_43160.PR'


plot_min_pressure = -0.1e-6
plot_max_pressure = 1e-6
if gauge_name == 'MKP.11955:PRESSURE':  plot_min_pressure, plot_max_pressure = -0.1e-7, 1e-6 
pressure_scale = "linear" #log/linear

date = "2023-03-24"
# date = "2023-04-20"

user_list = ['SPS:USER:MD5', 'SPS:USER:LHC25NS', 'SPS:USER:LHCMD3']
# user_list = ['SPS:USER:MD5', 'SPS:USER:LHC25NS']

# colors = ['b', 'r', 'k']
colors = [cm.tab10(i) for i in range(10)]

print('plotting for gauge ' + gauge_name)
save = "show"
# save = "save"

user_dict = {}
for ii, user in enumerate(user_list):
    print(f'here user is {user}')
    gauge_title = gauge_name #'VGHB_' + gauge
    plot_color = colors[ii]

    # if you want to use previous pickle files use the other format
    pickle_file = f'overview_pickles/overview_from_h5_{gauge_name}_{user}_SPS.BCTDC.51456_{date}.pkl'

    today = datetime.date.today().isoformat()

    ms = 3
    overview = pickle.load(open(pickle_file,'rb'))

    cycleStamps = np.array(overview['cycleStamps'])
    max_int = np.array(overview['max_intensity'])
    integrated_intensity = np.array(overview['integrated_intensity'])
    gauge_to_plot = np.array(overview['max_pressure_' + gauge_name])
    
    # mask = gauge_to_plot > 0
    mask = max_int > 1e11
    # mask = max_int > 3e13
    # mask = np.logical_and(mask, gauge_to_plot > 0)

    cycleStamps = cycleStamps[mask]
    max_int = max_int[mask]
    integrated_intensity = integrated_intensity[mask]
    gauge_to_plot = gauge_to_plot[mask]

    utc_dates = list(map(datetime.datetime.utcfromtimestamp, cycleStamps)) 

    gva_dates = [date + datetime.timedelta(hours=1) for date in utc_dates] ## Add 1 hour
        
    dates = matplotlib.dates.date2num(gva_dates)
    # plot_style = plot_color + '.'
    plot_style = '.'

    user_dict[user] = {'cycleStamps' : cycleStamps,
                       'max_int' : max_int,
                       'integrated_intensity' : integrated_intensity,
                       'gauge_to_plot' : gauge_to_plot,
                       'dates' : dates,
                       'plot_style' : plot_style,
                       'plot_color' : plot_color}

fig1 = plt.figure(1, figsize=[6.4*1.5, 4.8*1.5])  # user to be 1.2 and 1.3
ax1 = fig1.add_subplot(414)
for user in user_list:
    dates = user_dict[user]['dates']
    gauge_to_plot = user_dict[user]['gauge_to_plot']
    integrated_intensity = user_dict[user]['integrated_intensity']
    plot_style = user_dict[user]['plot_style']
    plot_color = user_dict[user]['plot_color']
    ax1.plot_date(dates, gauge_to_plot/integrated_intensity, plot_style, color=plot_color, label = user, ms=ms)
ax1.set_ylabel('Normalized pressure\n[mbar/p/s]')
#ax1.set_ylim(0,0.8e-18)
#ax1.set_xlabel('Date')
plt.xticks(rotation=40)
ax1.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d %H:%M'))
ax1.set_yscale('log')
plt.grid('on')
#ax1.set_xlabel('Date')
#fig1.subplots_adjust(bottom=0.2)
#plt.title('MKDVB')


ax2 = fig1.add_subplot(413, sharex=ax1, yscale=pressure_scale)
for uu, user in enumerate(user_list):
    dates = user_dict[user]['dates']
    gauge_to_plot = user_dict[user]['gauge_to_plot']
    #integrated_intensity = user_dict[user]['integrated_intensity']
    plot_style = user_dict[user]['plot_style']
    plot_color = user_dict[user]['plot_color']
    ax2.plot_date(dates, gauge_to_plot, plot_style, color=plot_color, label=user, ms=ms)
    # plot_color = colors[uu]
    # (markers, stemlines, baseline) = ax2.stem(dates, gauge_to_plot, label=user, basefmt=" ")
    # plt.setp(markers, markersize=3, color=plot_color)
    # plt.setp(stemlines, color=plot_color, linewidth=0.6, alpha=0.8)
#ax2.plot_date(dates, np.array(vghb_10660)/np.array(integrated_intensity), 'b.', label='VGHB_10660', ms=ms)
ax2.set_ylabel('Pressure [mbar]')
ax2.set_ylim(plot_min_pressure, plot_max_pressure)
#ax2.set_xlabel('Date')
plt.xticks(rotation=40)
ax2.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d %H:%M'))
# ax2.set_yscale('log')
plt.setp(ax2.get_xticklabels(), visible=False)
plt.grid('on')
#ax2.set_xticklabels([])
#plt.title('VGHB_10660') 
#plt.legend(loc='upper left')


ax3 = fig1.add_subplot(412, sharex=ax1, yscale='log')
for user in user_list:
    dates = user_dict[user]['dates']
    #gauge_to_plot = user_dict[user]['gauge_to_plot']
    integrated_intensity = user_dict[user]['integrated_intensity']
    plot_style = user_dict[user]['plot_style']
    plot_color = user_dict[user]['plot_color']
    ax3.plot_date(dates, np.array(integrated_intensity), plot_style, color=plot_color, label = user, ms=ms)
ax3.set_ylabel('Integrated \n intensity\n[p*s]')
plt.xticks(rotation=40)
ax3.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d %H:%M'))
#ax3.set_ylim(0, 1.6e12)
#ax3.set_xticklabels([])
plt.grid('on')
plt.setp(ax3.get_xticklabels(), visible=False)
#plt.title('MKDV1')
ax3.legend(loc='lower right')

#fig1.subplots_adjust(bottom=0.15)
#fig1.savefig('pressure_MKDV1.png', dpi=200)

ax4 = fig1.add_subplot(411, sharex=ax1)
for user in user_list:
    dates = user_dict[user]['dates']
    max_int = user_dict[user]['max_int']
    #integrated_intensity = user_dict[user]['integrated_intensity']
    plot_style = user_dict[user]['plot_style']
    plot_color = user_dict[user]['plot_color']
    ax4.plot_date(dates, np.array(max_int), plot_style, color=plot_color, label = user, ms=ms)
ax4.set_ylabel('Maximum intensity\n[p]')
plt.xticks(rotation=40)
ax4.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d %H:%M'))
#ax3.set_ylim(0, 1.6e12)
#ax3.set_xticklabels([])
plt.setp(ax4.get_xticklabels(), visible=False)
#plt.title('MKDV1')
plt.title(f'{gauge_title}')
plt.grid('on')
fig1.subplots_adjust(bottom=0.15)

if save == 'save':
    figName = 'pictures/pressure_' + gauge_name + '_' + today + '.png'
    print('saving figure as: ' + figName)
    fig1.savefig(figName, dpi = 200)
else:
    print('showing figure:')
    plt.show()


