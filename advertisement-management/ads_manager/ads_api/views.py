from rest_framework.views import APIView
from rest_framework import status
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from .serializers import ads_Serializer
from .models import advertisement
import pika
import boto3
import logging


AMQP_URL = 'amqps://sjrbzeex:ZfISj9WFzUNDbhV42IGWamNumXqwAQJI@hawk.rmq.cloudamqp.com/sjrbzeex'

logging.basicConfig(level=logging.INFO)

domain = 'https://c415769.parspack.net'
bucketName = 'c415769'
accessKey = 't9UZ8As8tZiIrMWr'
secretKey = 'k7cMprdc7fmQNNEeAVKcTOsY9OhwTCGs'

try:
    s3_resource = boto3.resource(
        's3',
        endpoint_url=domain,
        aws_access_key_id=accessKey,
        aws_secret_access_key=secretKey
    )
except Exception as exc:
    logging.info(exc)
else:

    bucket = s3_resource.Bucket(bucketName)


class ads_api_view(APIView):
  parser_classes = [MultiPartParser]

  def post(self, request):
    data = {
      'description': request.data.get('description'),
      'email': request.data.get('email')
    }
    serializer = ads_Serializer(data=data)
    if serializer.is_valid():
      serializer.save()
      ad_id = serializer.data['id']
      image = request.FILES.get('image', '')
      object_name = str(ad_id) + '.png'

      bucket.put_object(
            ACL='private',
            Body=image.read(),
            Key=object_name)
      print('Uploaded file: ', object_name)

      connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
      channel = connection.channel()
      channel.queue_declare(queue='ads_requests')
      channel.basic_publish(exchange='', routing_key='ads_requests', body=str(ad_id))
      connection.close()
      return Response(f'Your advertisement with id {ad_id} has been submitted.', status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

  def get(self, request):
    ad_id = advertisement.objects.get(id=request.data.get('id'))
    serializer = ads_Serializer(ad_id)
    if serializer.data.get('state') == 'pending':
      return Response('Your advertisement is in progress', status=status.HTTP_200_OK)
    elif serializer.data.get('state') == 'rejected':
      return Response('Your advertisement is not approved', status=status.HTTP_200_OK)
    else:
      return Response({'Your advertisement is accepted':serializer.data}, status=status.HTTP_200_OK)