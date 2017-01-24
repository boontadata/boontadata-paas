import datetime
from docdb_helper import DocDbHelper
import math
import numpy
import os
import pandas
from pandas.io.json import json_normalize
import time
import uuid

#connect to DocumentDb
docdbhelper = DocDbHelper()

#Compare Injector and downstream
inject_devicetime_results = docdbhelper.select("SELECT c.wt,c.di,c.c,c.sm1_inject_devicetime,c.sm2_inject_devicetime FROM c where c.sm1_inject_devicetime != null")
inject_sendtime_results = docdbhelper.select("SELECT * FROM c where c.sm1_inject_sendtime != null")
downstream_results = docdbhelper.select("SELECT * FROM c where c.sm1 != null")

df = pandas.DataFrame(columns=('window_time', 'device_id', 'category',
    'm1_sum_inject_sendtime', 'm1_sum_inject_devicetime', 'm1_sum_downstream',
    'm2_sum_inject_sendtime', 'm2_sum_inject_devicetime', 'm2_sum_downstream'))

i=1
for d in inject_devicetime_results:
    print(d['wt'], d['di'], d['c'], d['sm1_inject_devicetime'], d['sm2_inject_devicetime'])
    df[['window_time', 'device_id', 'category',
        'm1_sum_inject_sendtime', 'm2_sum_inject_sendtime'
        ]].radd([d['wt'], d['di'], d['c'], d['sm1_inject_devicetime'], d['sm2_inject_devicetime']])
    i += 1

print(df)
exit(0)


result = session.execute(
    "SELECT window_time, device_id, category, "
    + "m1_sum_inject_sendtime, m1_sum_inject_devicetime, m1_sum_downstream, "
    + "m2_sum_inject_sendtime, m2_sum_inject_devicetime, m2_sum_downstream "
    + "FROM agg_events ")
df = pandas.DataFrame(result[0])
df['delta_m1_sum_injectdevice_downstream'] = df.apply(lambda row: row.m1_sum_inject_devicetime - row.m1_sum_downstream, axis=1)
df['delta_m2_sum_injectdevice_downstream'] = df.apply(lambda row: row.m2_sum_inject_devicetime - row.m2_sum_downstream, axis=1)

#disconnect from Cassandra
cluster.shutdown()

pandas.set_option('display.height', 1000)
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 50)
pandas.set_option('display.width', 200)

print('showing all lines (not all columns) from aggregated events')
print('----------------------------------------------')
print(df
    .sort_values(by=['device_id', 'category', 'window_time'], axis=0, ascending=[True, True, True], inplace=False)
    .loc[:,['category', 'window_time', 'm1_sum_inject_devicetime', 'm1_sum_downstream', 
        'delta_m1_sum_injectdevice_downstream', 'm1_sum_inject_sendtime', 
        'm2_sum_inject_devicetime', 'm2_sum_downstream', 
        'delta_m2_sum_injectdevice_downstream', 'm2_sum_inject_sendtime',
        'device_id']])
print()

print('Comparing inject device and downstream for m1_sum')
print('--------------------------------------------------')
print("{} exceptions out of {}"
    .format(
        len(df.query('delta_m1_sum_injectdevice_downstream != 0').index),
        len(df.index)
    ))
print()

print('Exceptions are:')
print(
    df.query('delta_m1_sum_injectdevice_downstream != 0')
    .loc[:,['window_time', 'device_id', 'category', 'm1_sum_inject_devicetime', 'm1_sum_downstream', 'delta_m1_sum_injectdevice_downstream']]
    )

