from flask import Flask
from redis import Redis
import requests
import json
import os
import time
import socket

app = Flask(__name__)

@app.route('/')
def get_coin_price():
    if not redis.get(coin_name):
        page = ''
        while page == '':
            try:
                page = requests.get(host_api, headers=headers)
                break
            except:
                time.sleep(5)
                continue
        coin_json_response = page.json()
        coin_price = coin_json_response[0]['price_usd']
        redis.setex(coin_name, expiration_time, coin_price)
    price = redis.get(coin_name).decode("utf-8") 
    if return_host_name:
        host_name = socket.gethostname()
        return {'name': coin_name, 'price': price, 'hostname': host_name}
    else:
        return {'name': coin_name, 'price': price}

if __name__ == "__main__":

    if os.path.exists('./config-cluster.json'):
        with open('./config-cluster.json') as json_config_file:
            config_data = json.load(json_config_file)
    else:
        with open('./config.json') as json_config_file:
            config_data = json.load(json_config_file)

    server_port = config_data['SERVER_PORT']
    coin_name = config_data['COIN_NAME']
    expiration_time = config_data['EXPIRATION_TIME']
    host_api = config_data['HOST_API']
    api_key = config_data['API_KEY']
    headers = {'X-CoinAPI-Key': api_key}
    redis_host = config_data['REDIS_HOST']
    redis_port = config_data['REDIS_PORT']
    return_host_name = config_data['RETURN_HOST_NAME']
    redis = Redis(host=redis_host, port=redis_port)

    app.run(debug=True, host='0.0.0.0', port=server_port)
