import base64
import hashlib
import hmac
import json
import os
import requests
import time
import urllib

class IotHubHelper:
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT_WITH_POLICY = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'
    TOKEN_FORMAT_NO_POLICY = 'SharedAccessSignature sig=%s&se=%s&sr=%s'
    USAGE_CREATE_DEVICE=0
    USAGE_DEVICE_SENDS_MESSAGE=1

    def __init__(self, connectionString=None):
        try:
            self.iotHubConnectionString=os.environ['BOONTADATA_PAAS_iothub_registryrw_connectionstring']
        except Exception as ex:
            print("please set the following environment variables about IOT Hub")
            print("- BOONTADATA_PAAS_iothub_registryrw_connectionstring: an IOT Hub connection string with registry Read/Write access")
            raise ex
        iotHost, keyName, keyValue = [sub[sub.index('=') + 1:] for sub in self.iotHubConnectionString.split(";")]
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
            print('error: ' + str(r.status_code) + ' - ' + r.text)
            raise Exception
