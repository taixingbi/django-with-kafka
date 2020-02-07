from django.db import models
from django.conf import settings

class Account(models.Model):
    email = models.CharField(max_length=255)

    def __str__(self):
        return str(self.id)

    class Meta:
        db_table = 'account'




# class Example(models.Model):
#     account_id = models.IntegerField()
#     session_id = models.IntegerField()
#     slice_probabilities = models.TextField(max_length=100)
#     response_time = models.CharField(max_length=100)
#     threshold = models.FloatField()
#     PHQ8 = models.IntegerField()
#     name = models.CharField(max_length=100)

#     def __str__(self):
#         return str(self.account_id)

#     class Meta:
#        



