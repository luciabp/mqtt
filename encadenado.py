from paho.mqtt.client import Client
from paho.mqtt import publish
import traceback
from time import sleep
from math import sqrt
from multiprocessing import Process
import sys

data = {'media':[] , 'tiempo':[]}

def on_message(client, userdata, msg):
    print(msg.topic,msg.payload)
    message = str(msg.payload)
    try:
        if msg.topic in 'numbers':
            if int(sqrt(float(msg.payload)))**2 == float(msg.payload) and float(msg.payload) > 1:
                client.publish('clients/raices', msg.payload)
            else: 
                client.publish('clients/noraices', msg.payload)
        else:
            if msg.topic in 'clients/raices':
                data['tiempo'].append(int(sqrt(int(msg.payload))))
            else:
                data['media'].append(float(msg.payload))
    except Exception as e:
        print(e)
        traceback.print_exc()


client = Client()
client.on_message = on_message

client.connect("correo.mat.ucm.es")
topic = "numbers"
client.subscribe(topic)
client.subscribe('clients/raices')


client.loop_start()

while True:
    if data['tiempo'] != []:
        tiempo = data['tiempo'][0]
        data['tiempo'].pop(0)
        client.subscribe('clients/noraices')
        sleep(tiempo)
        client.unsubscribe('clients/noraices')
        media = sum(data['media'])/(max(1,len(data['media'])))
        mensaje = (f'La media durante {tiempo} seg en clients/noraices ha sido {media}')
        publish.single('clients/medias', payload = mensaje, hostname = "correo.mat.ucm.es")
        data['media'] = []

client.loop_close()