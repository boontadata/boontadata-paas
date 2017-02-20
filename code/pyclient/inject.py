use_print=True

import base64
import datetime
from docdb_helper import DocDbHelper
import getopt
#import hmac
#import hashlib
from iothub_helper import IotHubHelper
import json
import math
import numpy
import os
import pandas
import time
#import requests
#import urllib
import uuid
import sys

def gettimewindow(secondssinceepoch, aggwindowlength):
    dt=datetime.datetime.fromtimestamp(int(secondssinceepoch))
    return dt+aggwindowlength-datetime.timedelta(seconds=dt.second%aggwindowlength.seconds)

def senddata(iotHubHelper, docdbhelper, messageid, deviceid, devicetime, category, measure1, measure2, sendtime, patterncode):
    data='{"id":"%s", "di":"%s", "dt":"%s", "c":"%s", "m1":%s, "m2":%s}' % (
        messageid,
        deviceid, 
        str(devicetime), 
        category,
        str(measure1),
        str(measure2))
    if use_print:
        print(str(data), sendtime, str((sendtime-devicetime)/1000), patterncode)
    #write in IOT Hub
    iotHubHelper.sendMsg(deviceid, str(data).encode('utf-8'))

def sendaggdata(docdbhelper, deviceid, aggtype, aggdf):
    if use_print:
        print('aggregates of type ' + aggtype + ':')
        print(aggdf)
    aggdocs=[]
    for i,r in aggdf.iterrows():
        aggdoc = {'wt': str(i[0]), 
            'di': deviceid, 
            'c': i[1], 
            'sm1_inject_' + aggtype: int(r[0]), 
            'sm2_inject_' + aggtype: r[1]}
        aggdocs.append(aggdoc)
    docdbhelper.senddata(aggdocs)

def buildIoTHubSasToken(deviceId, iotHost, keyName, keyValue):
    
    resourceUri = '%s/devices/%s' % (iotHost, deviceId)
    targetUri = resourceUri.lower()
    expiryTime = '%d' % (time.time() + self.TOKEN_VALID_SECS)
    toSign = '%s\n%s' % (targetUri, expiryTime)
    key = base64.b64decode(keyValue.encode('utf-8'))
    signature = urllib.request.pathname2url(
        base64.b64encode(
            hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
        )
    ).replace('/', '%2F')
    
    return self.TOKEN_FORMAT % (signature, expiryTime, keyName, targetUri)

def createDeviceInIotHub(iotHubRegistryWriteConnectionString, deviceId):
    iotHost, iotKeyName, iotKeyValue = [sub[sub.index('=') + 1:] for sub in connectionStringOwner.split(";")]
    sasToken = self._buildIoTHubSasToken(deviceId, self.iotHostOwner, self.keyNameOwner, self.keyValueOwner)
    url = "https://%s/devices/%s?api-version=%s" % (self.iotHostOwner, deviceId, self.API_VERSION)
    message = '{deviceId: "%s"}' % deviceId
    r = requests.put(url, headers={'Content-Type': 'application/json', 'Authorization': sasToken}, data=message)
    response = json.loads(r.text)
    print(r.text)
    print(response['authentication']['symmetricKey']['primaryKey'])
    return r.text, r.status_code

def main():
    scriptusage='inject.py [-r <random-seed>] [-b <batch-size>]'
    randomseed=34
    batchsize=300
    m1max=100
    m2max=500
    basedelay=2*60*1000 #2 minutes
    aggwindowlength=datetime.timedelta(seconds=5)

    deviceid=str(uuid.uuid4())
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hr:b:",["random-seed=","batch-size="])
    except getopt.GetoptError:
        print(scriptusage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(scriptusage)
            sys.exit()
        elif opt in ("-r", "--random-seed"):
            randomseed = int(arg)
        elif opt in ("-b", "--batch-size"):
            batchsize = int(arg)

    print("randomseed={}, batchsize={}".format(randomseed, batchsize))

    #connect to IOT Hub
    iotHubHelper = IotHubHelper()
    iotHubHelper.createDevice(deviceid)

    #connect to DocumentDB
    docdbhelper=DocDbHelper()

    numpy.random.seed(randomseed)
    df = pandas.DataFrame({
        'measure1'   : numpy.random.randint(0, m1max, batchsize),
        'm2r'  : numpy.random.rand(batchsize),
        'catr' : numpy.random.randint(1,5,batchsize),
        'r1'   : numpy.random.rand(batchsize),
        'r2'   : numpy.random.rand(batchsize),
        'r3'   : numpy.random.rand(batchsize),
        'msgid': numpy.arange(0, batchsize, 1, dtype=int),
        'devicetime'  : numpy.array([0]*batchsize, dtype=int),
        'sendtime'    : numpy.array([0]*batchsize, dtype=int),
        'patterncode' : numpy.array(['']*batchsize)
    })
    df['category'] = df.apply(lambda row: "cat-{}".format(int(row['catr'])), axis=1)
    df['measure2'] = df.apply(lambda row: row['m2r'] * m2max, axis=1)
    df['messageid'] = df.apply(lambda row: "{}-{}".format(deviceid, int(row.msgid)), axis=1)
    df = df.drop(['catr', 'm2r', 'msgid'], axis=1)

    iappend=batchsize

    for i in range(0, batchsize):
        r = df.iloc[i]
        sendtime=int(round(time.time()*1000))
        patterncode=''
        if r.r1 < 0.01 :
            # late arrival, out of order
            devicetime=int(sendtime-basedelay-int(r.r2*1000*300)) #may add up to 300 additional seconds to the base delay
            patterncode='late' # devicetime < sendtime
        else:
            devicetime=sendtime
        df.loc[i, 'devicetime'] = devicetime
        df.loc[i, 'sendtime'] = sendtime
        df.loc[i, 'patterncode'] = patterncode
        senddata(iotHubHelper, docdbhelper, r.messageid, deviceid, devicetime, r.category, r.measure1, r.measure2, sendtime, patterncode)

        if r.r2 < 0.05 :
            #resend a previous message
            patterncode='re' # resend previous message
            resendindex = int(i*r.r1)
            sendtime = int(round(time.time()*1000))
            rbis = df.iloc[resendindex].copy()
            senddata(iotHubHelper, docdbhelper, rbis.messageid, deviceid, rbis.devicetime, rbis.category, rbis.measure1, rbis.measure2, sendtime, patterncode)
            rbis.sendtime=sendtime
            rbis.patterncode=patterncode
            df.loc[iappend] = rbis
            iappend+=1

        time.sleep(r.r3/10)

    # calculate aggregations from the sender point of view and send them to DocumentDB
    df = df.drop(['r1', 'r2', 'r3'], axis=1)
    df['devicetimewindow'] = df.apply(lambda row: gettimewindow(row.devicetime/1000, aggwindowlength), axis=1)
    df['sendtimewindow'] = df.apply(lambda row: gettimewindow(row.sendtime/1000, aggwindowlength), axis=1)

    sendaggdata(docdbhelper, deviceid, 'devicetime',
        df
            .query('patterncode != \'re\'')
            .groupby(['devicetimewindow', 'category'])['measure1', 'measure2']
            .sum())

    sendaggdata(docdbhelper, deviceid, 'sendtime',
        df
            .query('patterncode != \'re\'')
            .groupby(['sendtimewindow', 'category'])['measure1', 'measure2']
            .sum())

if __name__ == '__main__':
    main()
