"""
Module Name:  d2cMsgSender.py
Project:      IoTHubRestSample
Copyright (c) Microsoft Corporation.

Using [Send device-to-cloud message](https://msdn.microsoft.com/en-US/library/azure/mt590784.aspx) API to send device-to-cloud message from the simulated device application to IoT Hub.

This source is subject to the Microsoft Public License.
See http://www.microsoft.com/en-us/openness/licenses.aspx#MPL
All other rights reserved.

THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED 
WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.

this code comes from https://github.com/Azure-Samples/iot-hub-python-get-started/blob/master/Python/device/d2cMsgSender.py
and https://github.com/Azure-Samples/iot-hub-python-get-started/blob/master/Python/service/deviceManager.py
and was modified here
"""

import base64
import hmac
import hashlib
import os
import time
import requests
import urllib

import json

class D2CMsgSender:
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'
    
    def __init__(self, connectionStringDevice=None, connectionStringOwner=None):
        if connectionStringDevice != None:
            iotHostDevice, keyNameDevice, keyValueDevice = [sub[sub.index('=') + 1:] for sub in connectionStringDevice.split(";")]
            self.iotHostDevice = iotHostDevice
            self.keyNameDevice = keyNameDevice
            self.keyValueDevice = keyValueDevice
        if connectionStringOwner != None:
            iotHostOwner, keyNameOwner, keyValueOwner = [sub[sub.index('=') + 1:] for sub in connectionStringOwner.split(";")]
            self.iotHostOwner = iotHostOwner
            self.keyNameOwner = keyNameOwner
            self.keyValueOwner = keyValueOwner
            
    def _buildExpiryOn(self):
        return '%d' % (time.time() + self.TOKEN_VALID_SECS)
    
    def _buildIoTHubSasToken(self, deviceId, iotHost, keyName, keyValue):
        resourceUri = '%s/devices/%s' % (iotHost, deviceId)
        targetUri = resourceUri.lower()
        expiryTime = self._buildExpiryOn()
        toSign = '%s\n%s' % (targetUri, expiryTime)
        key = base64.b64decode(keyValue.encode('utf-8'))
        signature = urllib.request.pathname2url(
            base64.b64encode(
                hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
            )
        ).replace('/', '%2F')
       
        return self.TOKEN_FORMAT % (signature, expiryTime, keyName, targetUri)
    
    def sendD2CMsg(self, deviceId, message):
        sasToken = self._buildIoTHubSasToken(deviceId, self.iotHostDevice, self.keyNameDevice, self.keyValueDevice)
        url = "https://%s/devices/%s/messages/events?api-version=%s" % (self.iotHostDevice, deviceId, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message)
        return r.text, r.status_code

    def createDevice(self, deviceId):
        sasToken = self._buildIoTHubSasToken(deviceId, self.iotHostOwner, self.keyNameOwner, self.keyValueOwner)
        url = "https://%s/devices/%s?api-version=%s" % (self.iotHostOwner, deviceId, self.API_VERSION)
        message = '{deviceId: "%s"}' % deviceId
        r = requests.put(url, headers={'Content-Type': 'application/json', 'Authorization': sasToken}, data=message)
        response = json.loads(r.text)
        print(r.text)
        print(response['authentication']['symmetricKey']['primaryKey'])

        return r.text, r.status_code
    
if __name__ == '__main__':
    connectionStringDevice = os.environ['BOONTADATA_PAAS_iothub_device_connectionstring']
    connectionStringOwner = os.environ['BOONTADATA_PAAS_iothub_owner_connectionstring']
    d2cMsgSender = D2CMsgSender(connectionStringDevice, connectionStringOwner)
    deviceId = 'iotdevice5'

    print(d2cMsgSender.createDevice(deviceId))

    message = 'Hello, IoT Hub'
    print(d2cMsgSender.sendD2CMsg(deviceId, message))
