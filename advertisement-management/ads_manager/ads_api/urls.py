from django.urls import path, include
from .views import ads_api_view

urlpatterns = [path('api', ads_api_view.as_view())]

