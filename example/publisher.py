from __future__ import print_function

import os
import time
import gevent
import datetime

from nucleon.amqp import Connection


conn = Connection(
    os.environ.get('AMQP_URL', 'amqp://guest:guest@localhost/'),
    heartbeat=5
)
last_num = 1


@conn.on_connect
def on_connect(conn):
    global last_num
    with conn.channel() as channel:
        channel.exchange_declare(
            exchange='example',
            type='direct',
            durable=True
        )
        channel.confirm_select()
        while True:
            channel.basic_publish(
                exchange='example',
                routing_key='counter',
                body=str(last_num),
                headers={
                    'delivery_mode': 2,
                    'time': str(time.time())
                }
            )
            print(
                datetime.datetime.now().strftime('[%H:%M:%S]'),
                "Published message '%d'" % last_num
            )
            last_num += 1
            gevent.sleep(1)


if __name__ == '__main__':
    conn.connect()

    try:
        conn.join()
    except KeyboardInterrupt:
        conn.close()
