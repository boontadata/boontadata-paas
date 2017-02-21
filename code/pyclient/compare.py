import datetime
from docdb_helper import DocDbHelper
import math
import numpy
import os
import pandas
from pandas.io.json import json_normalize
import time
import uuid

print("compare V170221a")

#connect to DocumentDb
docdbhelper = DocDbHelper()

#Compare Injector and downstream
inject_devicetime_results = pandas.DataFrame(list(docdbhelper.select(
    "SELECT c.wt as window_time, c.di as device_id, c.c as category, "
    + "c.sm1_inject_devicetime as m1_sum_inject_devicetime, "
    + "c.sm2_inject_devicetime as m2_sum_inject_devicetime "
    + "FROM c where c.sm1_inject_devicetime != null")))

inject_sendtime_results = pandas.DataFrame(list(docdbhelper.select(
    "SELECT c.wt as window_time, c.di as device_id, c.c as category,"
    + "c.sm1_inject_sendtime as m1_sum_inject_sendtime,"
    + "c.sm2_inject_sendtime as m2_sum_inject_sendtime "
    + "FROM c where c.sm1_inject_sendtime != null")))

downstream_results = pandas.DataFrame(list(docdbhelper.select(
    "SELECT c.wt as window_time, c.di as device_id, c.c as category,"
    + "c.sm1 as m1_sum_downstream,"
    + "c.sm2 as m2_sum_downstream "
    + ", c.nbevents as nbevents_downstream "
    + "FROM c where c.sm1 != null")))
downstream_results["window_time"] = downstream_results.apply(lambda row: row.window_time.replace('T', ' ').replace('Z', ''), axis=1)

df = pandas.merge(inject_devicetime_results, inject_sendtime_results, how='left', on=['category', 'device_id', 'window_time'])
df = pandas.merge(df, downstream_results, how='left', on=['category', 'device_id', 'window_time'])

df['delta_m1_sum_injectdevice_downstream'] = df.apply(lambda row: row.m1_sum_inject_devicetime - row.m1_sum_downstream, axis=1)
df['delta_m2_sum_injectdevice_downstream'] = df.apply(lambda row: row.m2_sum_inject_devicetime - row.m2_sum_downstream, axis=1)

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
        'nbevents_downstream',
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

