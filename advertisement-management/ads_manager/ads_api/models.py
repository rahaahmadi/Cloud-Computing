from django.db import models

# Create your models here.

class advertisement(models.Model):
    id = models.AutoField(primary_key=True)
    description = models.CharField(max_length=500)
    email = models.CharField(max_length=100)
    state = models.CharField(max_length=50, blank=True, null=True, default='pending')
    category = models.CharField(max_length=100, blank=True, null=True)