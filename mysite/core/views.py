import os
import json

import logging
logger = logging.getLogger(__name__)
from django.http import HttpResponse

from django.http import Http404
from django.http import HttpResponseServerError
from django.core.exceptions import EmptyResultSet

from django.shortcuts import render, redirect
from django.views.generic import TemplateView, ListView, CreateView
from django.http import JsonResponse
from django.utils import timezone

 #kafka
from kafkaStream.core import KafkaClass

# from django.contrib.staticfiles import finders
import pandas as pd 

 #db
from database.orm import DBRead

#s3
from aws.s3 import s3Bucket
from aws.ses import SES

from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import logout
from django.views.decorators.csrf import csrf_exempt


import time

from django.conf import settings

from kafka import KafkaConsumer



dataTime= {
    "time": timezone.localtime(),
}

# views
class Home(TemplateView):
    template_name = 'home.html'

#logout
def logout_view(request):
    logout(request)
    return render(request,'home.html')

# -----------------------------------------for api-------------------------------------
class Demo(): 

    #/test/
    @csrf_exempt
    def test(request):  
        print("\n\n*************************************api test*************************************")

        # if request.method == 'POST':
        #     dataJson=json.loads(request.body)
        #     print(dataJson)

        # return JsonResponse(dataJson)

        KafkaClass().convertVideoFsX()
        #KafkaClass().convertVideoStr()

        dataJson= {
            "test1": "test"
        }
        return JsonResponse(dataJson)


    #/test/s3/
    def s3(request):  
        print("\n\n************************************* s3 test*************************************")

        #bucket='thrivee-dev/audiotranscribe'
        bucket=  'thrivee-dev'

        key= 'audiotranscribe/test.wav'

        fileName= 'media/' + key.split('/')[1]
        print(fileName)
        res= s3Bucket(bucket, key, fileName).loadFile()

        data= {
            "s3": res,
        }
        
        return JsonResponse(data)
    
    #/test/db
    def db(request):  
        print("\n\n************************************* db test*************************************")
        email= DBRead().test() 

        data= {
            "db test": "success",
            "email": email,
        }
        
        return JsonResponse(data)

    def ses(request):  
        print("\n\n************************************* ese test*************************************")

        SES().gmail()

        data= {
            "ses": "ses",
        }
        
        return JsonResponse(data)


    #/api/demo
    def demo(request):  #s3 key
        print("\n\n*************************************demo api Service *************************************")

        data= {
            "demo": "demo",
        }
        
        return JsonResponse(data)

        # df= pd.DataFrame(data, index=[0])
        # return HttpResponse( df.to_html() )
    

    #/api/producer
    @csrf_exempt
    def producer(request):  #s3 key
        print("\n\n*************************************producer api Service *************************************")
        print( request.method )

        if request.method == 'POST':
            data=json.loads(request.body) #json
            print(data)
            topic, dataJson= KafkaClass().producerJson(data)

        # data= {
        #     "topic": "topic",
        #     "value": "data",
        # }
        
        return JsonResponse(data)

        # df= pd.DataFrame(data, index=[0])
        # return HttpResponse( df.to_html() )     

    #/api/consumer
    def consumer(request):  #s3 key
        print("\n\n*************************************consumer api Service *************************************")
        KafkaClass().consumerJson()

        data= {
            "consumer": "consumer",
        }
        
        return JsonResponse(data)

        # df= pd.DataFrame(data, index=[0])
        # return HttpResponse( df.to_html() )  
