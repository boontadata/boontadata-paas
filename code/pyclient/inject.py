use_documentDb=False
use_print=True

import base64
import datetime
import getopt
import hmac
import hashlib
import json
import math
import numpy
import os
import pandas
import time
import requests
import urllib
import uuid
import sys

class IotHubSender:
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT_WITH_POLICY = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'
    TOKEN_FORMAT_NO_POLICY = 'SharedAccessSignature sig=%s&se=%s&sr=%s'
    USAGE_CREATE_DEVICE=0
    USAGE_DEVICE_SENDS_MESSAGE=1

    def __init__(self, connectionString=None):
        if connectionString != None:
            iotHost, keyName, keyValue = [sub[sub.index('=') + 1:] for sub in connectionString.split(";")]
            self.iotHost = iotHost
            self.initialKeyName = keyName
            self.initialKeyValue = keyValue

    def _buildExpiryOn(self):
        return '%d' % (time.time() + self.TOKEN_VALID_SECS)
    
    def _buildIoTHubSasToken(self, deviceId, usage):
        if usage==self.USAGE_CREATE_DEVICE:
            keyValue=self.initialKeyValue
        elif usage==self.USAGE_DEVICE_SENDS_MESSAGE:
            keyValue=self.currentDeviceKey
        else:
            keyVale
        resourceUri = '%s/devices/%s' % (self.iotHost, deviceId)
        targetUri = resourceUri.lower()
        expiryTime = self._buildExpiryOn()
        toSign = '%s\n%s' % (targetUri, expiryTime)
        key = base64.b64decode(keyValue.encode('utf-8'))
        signature = urllib.request.pathname2url(
            base64.b64encode(
                hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
            )
        ).replace('/', '%2F')
        if usage==self.USAGE_CREATE_DEVICE:
            return self.TOKEN_FORMAT_WITH_POLICY % (signature, expiryTime, self.initialKeyName, targetUri)
        elif usage==self.USAGE_DEVICE_SENDS_MESSAGE:
            return self.TOKEN_FORMAT_NO_POLICY % (signature, expiryTime, targetUri)
        else:
            return None

    def sendMsg(self, deviceId, message):
        sasToken = self._buildIoTHubSasToken(deviceId, self.USAGE_DEVICE_SENDS_MESSAGE)
        url = "https://%s/devices/%s/messages/events?api-version=%s" % (self.iotHost, deviceId, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message)
        return r.text, r.status_code

    def createDevice(self, deviceId):
        sasToken = self._buildIoTHubSasToken(deviceId, self.USAGE_CREATE_DEVICE)
        url = "https://%s/devices/%s?api-version=%s" % (self.iotHost, deviceId, self.API_VERSION)
        message = '{deviceId: "%s"}' % deviceId
        r = requests.put(url, headers={'Content-Type': 'application/json', 'Authorization': sasToken}, data=message)
        if r.status_code == 200:
            response = json.loads(r.text)
            self.currentDeviceKey=response['authentication']['symmetricKey']['primaryKey']
        else:
            self.currentDeviceKey=None
            print(r)
            raise Exception

def gettimewindow(secondssinceepoch, aggwindowlength):
    dt=datetime.datetime.fromtimestamp(int(secondssinceepoch))
    return dt+aggwindowlength-datetime.timedelta(seconds=dt.second%aggwindowlength.seconds)

def senddata(iotHubSender, dbSender, messageid, deviceid, devicetime, category, measure1, measure2, sendtime, patterncode):
    data="|".join([
        messageid,
        deviceid, 
        str(devicetime), 
        category,
        str(measure1),
        str(measure2)])

    if use_print:
        print(str(data), sendtime, str((sendtime-devicetime)/1000), patterncode)

    #write in IOT Hub
    iotHubSender.sendMsg(deviceid, str(data).encode('utf-8'))

    if use_documentDb:
        dbSender.execute("INSERT INTO raw_events "
            + "(message_id, device_id, device_time, send_time, category, measure1, measure2) "
            + "VALUES ('{}', '{}', {}, {}, '{}', {}, {})"
            .format(messageid, deviceid, devicetime, sendtime, category, measure1, measure2))

def sendaggdata(dbSender, deviceid, aggtype, aggdf):
    if use_print:
        print('aggregates of type ' + aggtype + ':')
        print(aggdf)
    
    if use_documentDb:
        for i,r in aggdf.iterrows():
            # upsert by inserting (cf http://www.planetcassandra.org/blog/how-to-do-an-upsert-in-cassandra/)
            dbSender.execute("INSERT INTO agg_events "
                + "(window_time, device_id, category, m1_sum_{0}_{1}, m2_sum_{0}_{1}) VALUES ('{2}', '{3}', '{4}', {5}, {6})"
                .format('ingest', aggtype, 
                    str(i[0]), deviceid, i[1],
                    int(r[0]), r[1]))

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
    scriptusage='ingest.py [-r <random-seed>] [-b <batch-size>] -c <iot-hub-connection-string> (iot-hub-connection-string needs registry write access)'
    randomseed=34
    batchsize=300
    m1max=100
    m2max=500
    basedelay=2*60*1000 #2 minutes
    aggwindowlength=datetime.timedelta(seconds=5)
    iotHubConnectionString=None

    deviceid=str(uuid.uuid4())
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hr:b:c:",["random-seed=","batch-size=","iot-hub-connection-string"])
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
        elif opt in ("-c", "--iot-hub-connection-string"):
            iotHubConnectionString = arg

    if iotHubConnectionString==None:
        print(scriptusage)
        sys.exit(2)

    print("randomseed={}, batchsize={}, iotHubConnectionString={}", 
        randomseed, batchsize, iotHubConnectionString)

    #connect to IOT Hub
    iotHubSender = IotHubSender(iotHubConnectionString)
    iotHubSender.createDevice(deviceid)

    #connect to DocumentDB
    dbSender=None #TODO

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
        senddata(iotHubSender, dbSender, r.messageid, deviceid, devicetime, r.category, r.measure1, r.measure2, sendtime, patterncode)

        if r.r2 < 0.05 :
            #resend a previous message
            patterncode='re' # resend previous message
            resendindex = int(i*r.r1)
            sendtime = int(round(time.time()*1000))
            rbis = df.iloc[resendindex].copy()
            senddata(iotHubSender, dbSender, rbis.messageid, deviceid, rbis.devicetime, rbis.category, rbis.measure1, rbis.measure2, sendtime, patterncode)
            rbis.sendtime=sendtime
            rbis.patterncode=patterncode
            df.loc[iappend] = rbis
            iappend+=1

        time.sleep(r.r3/10)

    # calculate aggregations from the sender point of view and send them to Cassandra
    df = df.drop(['r1', 'r2', 'r3'], axis=1)
    df['devicetimewindow'] = df.apply(lambda row: gettimewindow(row.devicetime/1000, aggwindowlength), axis=1)
    df['sendtimewindow'] = df.apply(lambda row: gettimewindow(row.sendtime/1000, aggwindowlength), axis=1)

    sendaggdata(dbSender, deviceid, 'devicetime',
        df
            .query('patterncode != \'re\'')
            .groupby(['devicetimewindow', 'category'])['measure1', 'measure2']
            .sum())

    sendaggdata(dbSender, deviceid, 'sendtime',
        df
            .query('patterncode != \'re\'')
            .groupby(['sendtimewindow', 'category'])['measure1', 'measure2']
            .sum())

    #disconnect from DocumentDB
    #XXX

if __name__ == '__main__':
    main()
