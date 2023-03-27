import os
import sys
import requests
import pika
import psycopg2

BUCKET_URL = 'https://c415769.parspack.net/c415769/'

AMQP_URL = 'amqps://sjrbzeex:ZfISj9WFzUNDbhV42IGWamNumXqwAQJI@hawk.rmq.cloudamqp.com/sjrbzeex'

DATABASE_URL = 'postgres://avnadmin:AVNS_tpbPI0Bj17beAqsxLMO@pg-1b3157d2-r4ha-19c3.aivencloud.com:25285/defaultdb?sslmode=require'

api_key = 'acc_fab55427a24870b'
api_secret = 'c5c0a7a86f4b73aa6f44d76a8077c301'

def send_simple_message(to_email,result,body_str):
  return requests.post(
    "https://api.mailgun.net/v3/sandbox4985e69625a84a6786206bf3b305ea67.mailgun.org/messages",
    auth=("api", "809541c9205a633df7566bad4a8c86b2-48c092ba-3273f86f"),
    data={"from": "ad manager admin <postmaster@sandbox4985e69625a84a6786206bf3b305ea67.mailgun.org>",
          "to": [to_email],
          "subject": "AD MANAGER",
          "text": f'your ad with id {body_str} has been {result.capitalize()}'})

def main():
  connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
  channel = connection.channel()
  channel.queue_declare(queue='ads_requests')
  conn = psycopg2.connect(DATABASE_URL)

  def callback(ch, method, properties, body):
    body_str = str(int(body))
    image_url = BUCKET_URL + body_str + '.png'
    response = requests.get('https://api.imagga.com/v2/tags?image_url=%s' % image_url, auth=(api_key, api_secret), verify='/Users/raha/Downloads/certs.pem')

    state = ''
    category = ''

    tags = response.json()['result']['tags']
    for tag in tags:
      if tag['tag']['en'] == 'vehicle' and tag['confidence'] > 50:
        category = tags[0]['tag']['en']
        state = 'accepted'
        break
      else:
        state = 'rejected'

    cur = conn.cursor()
    cur.execute('UPDATE ads_api_advertisement SET (state, category) = (%s, %s) WHERE id = %s;', (state, category, body_str))
    conn.commit()

    cur.execute('SELECT email FROM ads_api_advertisement WHERE id = %s;', (body_str, ))
    user_email = cur.fetchall()
    cur.close()
    send_simple_message(user_email[0][0], state, body_str)
    print(f'Advertisement {body_str} has been processed\n')

  channel.basic_consume(queue='', on_message_callback=callback, auto_ack=True)
  print(' [*] Waiting for messages. To exit press CTRL+C')
  channel.start_consuming()
  conn.close()


if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Interrupted')
    try:
      sys.exit(0)
    except SystemExit:
      os._exit(0)
