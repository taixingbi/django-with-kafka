from django.conf import settings

from kafka import KafkaProducer
from kafka import KafkaConsumer

from kafka.errors import KafkaError
import json

import pandas as pd
import numpy as np

import base64
from .audio import audio

from scipy.io import wavfile

class KafkaClass:
    def __init__(self):
        print("\nKafkaCore...")

    ## Fs X
    # def readAudioFsX(self):
    #     print("\nreadAudio...")
    #     filename= settings.MEDIA_ROOT

    #     filename += '/test.wav'
    #     print(filename)

    #     fs, x = wavfile.read(filename)
    #     # print( type(fs), type(x))
    #     # print(fs, x[-10:], x.shape )

    #     return fs, x

    # def writeAudioFsX(self, fs, x):
    #     print("\nwriteAudio...")

    #     filename= settings.MEDIA_ROOT
    #     filename += '/testFsX.wav'
    #     print(filename)

    #     # print( type(fs), type(x) )
    #     # print(fs, x[-10:], x.shape )

    #     wavfile.write(filename, fs, x)

    #     return True

    # def convertVideoFsX(self):
    #     print("\nconvertVideo..")
    #     fs, d=self.readAudioFsX()

    #     print("-----------------")
    #     x= d.tolist()
    #     print( x[-10:] )
    #     d= np.array( x,  dtype=np.int16)
    #     print( d[-10:] )

    #     print("-----------------")

    #     self.writeAudioFsX(fs, d)

    # ## Str
    # def readAudioStr(self):
    #     print("\nreadAudioStr...")
    #     filename= settings.MEDIA_ROOT + '/test.wav'

    #     f = open(filename, "rb")
    #     data= f.read()
    #     f.close()

    #     #print(type(data))
    #     return data

    # def writeAudioStr(self, data):
    #     print("\nwriteAudioStr...")

    #     filename= settings.MEDIA_ROOT + '/testStr.wav'
    #     f = open(filename, 'w+b')
    #     f.write(bytearray(data))
    #     f.close()

    #     return True

    # def convertVideoStr(self):
    #     print("\nconvertVideoStr..")
    #     data= self.readAudioStr()
    #     self.writeAudioStr(data)


    def producerJson(self, data):
        print("\nproducer..")

        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        topic= 'test'

        # ----------------------send key and value----------------------
        #data=self.readAudioStr()
        #producer.send('test', key=b'audio', value= data)  #value= b'v2'

        # ----------------------send json----------------------
        #fs, x=self.readAudioFsX()

        # dataJson= \
        # {
        #     'topic': topic,
        #     'fs': fs,
        #     'x': x.tolist()
        # }

        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send(topic, data)

        # -------------------read to send -------------------
        producer.flush()

        #print(topic, dataJson)

        return topic, data

    def consumerJson(self):
        print("\nconsumer..")

        # To consume latest messages and auto-commit offsets
        consumer = KafkaConsumer('test',
                                group_id='my-group',
                                bootstrap_servers=['localhost:9092'])
        for message in consumer:
            print("\n -----------------consumer message-----------------")
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`

            # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
            #                                     message.offset, message.key,
            #                                     message.value))
            
            topic= message.topic 
            print("topic ", topic)

            b= message.value
            print(b)
            # ----------------------message key and value----------------------
            # print(message.key)
            # try:
            #     self.writeAudioStr(b)
            # except:
            #     print("not key and value")

            # ----------------------message json----------------------

            # try:
            #print("json object")

            dataStr = b.decode('utf8').replace("'", '"')

            data = json.loads(dataStr) # str2json
            print(data)

            fs, x= data['fs'], np.array( data['x'], dtype=np.int16 )

            filename= settings.MEDIA_ROOT + '/'  + data['filename']
            print(filename)
            audio().writeAudioFsX(fs, x, filename)


            #self.writeAudioFsX(fs, x)

            # except:
            #     print("not json")

            print("consumer done")





            # # consume earliest available messages, don't commit offsets
            # KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

            # # consume json messages
            # KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

            # # # consume msgpack
            # # KafkaConsumer(value_deserializer=msgpack.unpackb)

            # # StopIteration if no message after 1sec
            # KafkaConsumer(consumer_timeout_ms=1000)

            # # Subscribe to a regex topic pattern
            # consumer = KafkaConsumer()
            # consumer.subscribe(pattern='^awesome.*')

  

        # consumer = KafkaConsumer('test',
        #                         group_id='my-group',
        #                         bootstrap_servers=['localhost:9092'])
        # for message in consumer:
        #     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                         message.offset, message.key,
        #                                         message.value))

        # KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))



if __name__ == "__main__": 
    # print("print kafkaStram...")
    # KafkaClass().producer()

    KafkaClass().convertVideoFsX()

        

        
