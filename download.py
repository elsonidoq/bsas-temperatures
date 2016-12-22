import argparse
import json
from urllib2 import urlopen

from datetime import datetime, timedelta
import pymongo
from rabbit_mq import RabbtMQ
from fito.data_store.mongo import MongoHashMap

cache = MongoHashMap(pymongo.MongoClient().temperatures.by_hour)


@cache.cache()
def get_for_date(station_id, day, month, year):
    url_template = 'http://www.meteored.com.ar/peticiones/datosgrafica_sactual_16.php?id_estacion={station_id}&accion=T&id_localidad=13584&anno={year}&mes={month}&dia={day}'
    url = url_template.format(
        month=month,
        day=day,
        year=year,
        station_id=station_id,
    )
    response = urlopen(url).read()
    if response: return eval(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['clean', 'listen', 'push'])

    args = parser.parse_args()
    if args.mode == 'listen':
        def process(body):
            get_for_date(**body)

        with RabbtMQ().connection() as connection:
            print "Waiting..."
            connection.receive_socket('get_temperatures', process)

    elif args.mode == 'push':
        d = datetime.now()
        available_stations = [
            # '571e07bcc76c49177837d5d4',
            # '571e07bdc76c49177837d604',
            # '571e07bcc76c49177837d5e9',
            # '571e07bcc76c49177837d5e6',
            '571e079bc76c49177837cd9b',

        ]
        with RabbtMQ().connection() as connection:
            for i in xrange(20 * 365):
                for station_id in available_stations:
                    d -= timedelta(days=1)
                    connection.push_socket('get_temperatures', {
                        'day': d.day,
                        'month': d.month,
                        'year': d.year,
                        'station_id': station_id,
                    })
    else:
        with RabbtMQ().connection() as connection:
            connection.channel.queue_purge('get_temperatures')


if __name__ == '__main__': main()


