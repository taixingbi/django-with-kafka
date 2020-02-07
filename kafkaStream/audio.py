import requests
import json
from scipy.io import wavfile

class audio:
    def __init__(self):
        print("\nKafkaCore...")

    ## Fs X
    def readAudioFsX(self, filename):
        print("\nreadAudio...")

        print(filename)
        fs, x = wavfile.read(filename)
        # print( type(fs), type(x))
        # print(fs, x[-10:], x.shape )

        return fs, x

    def writeAudioFsX(self, fs, x, filename):
        print("\nwriteAudio...")

        print(filename)

        # print( type(fs), type(x) )
        # print(fs, x[-10:], x.shape )

        wavfile.write(filename, fs, x)

        return True

    def convertVideoFsX(self):
        print("\nconvertVideo..")
        fs, d=self.readAudioFsX()

        print("-----------------")
        x= d.tolist()
        print( x[-10:] )
        d= np.array( x,  dtype=np.int16)
        print( d[-10:] )

        print("-----------------")

        self.writeAudioFsX(fs, d)

    ## Str
    def readAudioStr(self, filename):
        print("\nreadAudioStr...")

        print(filename)

        f = open(filename, "rb")
        data= f.read()
        f.close()

        #print(type(data))
        return data

    def writeAudioStr(self, data, filename):
        print("\nwriteAudioStr...")

        print(filename)

        f = open(filename, 'w+b')
        f.write(bytearray(data))
        f.close()

        return True

    def convertVideoStr(self):
        print("\nconvertVideoStr..")

        filename= '/Users/h/Desktop/audio-samples/test1.wav'
        data= self.readAudioStr(filename)
        self.writeAudioStr(data)

    def requestApi(self, url, data):
        print("\nrequestApi..")
        print(data)

        data= json.dumps(data) # json2str
        response = requests.post(url, data = data)
        #response = requests.get(url)

        print(response)
        if response.status_code == 200:
            print(url, "200 ok")





        print("done")


if __name__ == "__main__": 
    print("print audio...")

    filename= '/Users/h/Desktop/audio-samples/test1.wav'
    fs, x= audio().readAudioFsX(filename)
    print(fs)

    filename= filename.split("/")[-1]
    x= x.tolist()
    #x= [1,2,3,4]

    data= \
    {
        "topic": "test",
        "filename": filename,
        "fs": fs,
        "x": x,
        "account_id": 367,
        "session_id": 406,
        "audio": "reserved"
    }

    url= 'http://0.0.0.0:8083/api/producer/'

    audio().requestApi(url, data)






