#!/usr/bin/env python
import pika
import subprocess


def send_message(message=''):
        connection = pika.BlockingConnection(pika.connection.URLParameters(
        "amqp://kraken:guest@10.152.10.149:7777/kraken_vhost"))
        channel = connection.channel()
        channel.queue_declare(queue='hello')
        channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=message)
        connection.close()

proc = subprocess.Popen(['stap', '../stp_scripts/query.stp'], stdout=subprocess.PIPE)
while True:
        line = proc.stdout.readline()
        if line != '':
                send_message(line.rstrip())
        else:
                break


