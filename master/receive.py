#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.connection.URLParameters(
        "amqp://kraken:guest@10.152.10.149:7777/kraken_vhost"))
channel = connection.channel()


channel.queue_declare(queue='hello')
def handle_message(message=''):
    pid, func = message.split("|", 2)
    
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
