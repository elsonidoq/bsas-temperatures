import json
from contextlib import contextmanager
from time import sleep
from time import time

import pika
from bson import json_util
from pika.exceptions import ConnectionClosed


class RabbtMQ:
    def __init__(self):
        self.logger = BasicLogger(prefix='[RabbtMQ]')
        self.url = 'localhost'
        self.full_retries = 2

    def push_socket(self, queue_name, data):
        with(self.connection()) as connection:
            connection.push_socket(queue_name, data)

    @contextmanager
    def connection(self, auto_ack=True):
        connection = self.connect()
        yield (RabbitConnection(connection, auto_ack=auto_ack))

    def connect(self, try_num=0):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(self.url))
        except Exception as e:
            if try_num < 10:
                self.logger.log('Reconnecting')
                return self.connect(try_num + 1)
            else:
                if self.full_retries > 0:
                    self.logger.log('Failed 10 times...waiting a bit')
                    sleep(2)
                    self.full_retries -= 1
                    return self.connect()
                else:
                    raise e


class RabbitConnection:
    def __init__(self, connection, auto_ack=True):
        self.logger = BasicLogger(prefix='[RabbtMQ]')
        self.connection = connection
        self._channel = None
        self.last_print = time()
        self.auto_ack = auto_ack

    def receive_socket(self, queue_name, callback):
        while True:
            try:
                self._receive_socket(queue_name, callback)
            except ConnectionClosed:
                pass

    def _receive_socket(self, queue_name, callback):
        channel = self.connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queue=queue_name, durable=True)

        def callback_wrapper(ch, method, properties, body):
            body = body.decode('utf8')
            if time() - self.last_print > 1:
                print(" [x] Received %s" % body)
                self.last_print = time()
            callback(json_util.loads(body))
            if self.auto_ack: channel.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(callback_wrapper,
                              queue=queue_name,
                              no_ack=False)

        print(' [*] Waiting for messages from %s. To exit press CTRL+C' % queue_name)
        channel.start_consuming()

    @property
    def channel(self):
        if self._channel is None:
            self._channel = self.connection.channel()
        return self._channel

    def push_socket(self, queue_name, data):
        json_data = json_util.dumps(data)
        if time() - self.last_print > 1:
            self.logger.log("Sending %s to %s" % (json_data, queue_name))
            self.last_print = time()
        self.channel.basic_publish(exchange='', routing_key=queue_name, body=json_data)


class BasicLogger():
    def __init__(self, prefix=''):
        self.prefix = prefix + " " if prefix else ''

    def log(self, text):
        print(self._build_log(text))

    def jsonlog(self, obj):
        self.log('')
        try:
            print(json.dumps(obj, sort_keys=True, indent=2))
        except TypeError as e:
            from bson import json_util
            json_util.dumps(obj, sort_keys=True, indent=2)

    def _build_log(self, text):
        return "%s%s" % (self.prefix, text)