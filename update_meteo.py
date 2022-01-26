#!/usr/bin/python3
from contextlib import closing
from urllib.request import urlopen
import json
from datetime import datetime
import time
import random
import logging
import psycopg2
from psycopg2.errors import SerializationFailure
from grootgle_tools import grootgle


#Variables à changer
token="cadafc65fe8e1ab52c15dccfd3fc39b446cf8a46f7af0362bc75f67e7320f438"
code_insee="78356"
#find the local station here https://api.meteo-concept.com/carte-des-stations
#local_station_uuid="0a2e6476-bd6f-4792-b2a3-4a319b8e4afb"
####################

url_ephemeride=('https://api.meteo-concept.com/api/ephemeride/0?token={}&insee={}').format(token,code_insee)
url_weather_data=('https://api.meteo-concept.com/api/observations/around?token={}&insee={}&radius=5&sorting=asc').format(token,code_insee)

def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    localconn = psycopg2.connect("dbname=aquarium user=pi")

    try:
        last_synced_local_ephemeride=grootgle.last_sync_datas(localconn, "ephemeride")
        now = datetime.now()

        if (now-last_synced_local_ephemeride).days > 0:
            with closing(urlopen(url_ephemeride)) as f:
                cityEph = json.loads(f.read())
            grootgle.insert_ephemeride(localconn,cityEph)
        else:
            logging.debug("ephemeride already catched for today")

        last_synced_local_outside_weather = grootgle.last_sync_datas(localconn, "outside_weather", " where 1=1")

        with closing(urlopen(url_weather_data)) as f:
            decoded = json.loads(f.read())
            if decoded:
                for current in decoded:
                    (station, observation) = (current[k] for k in ('station', 'observation'))
                    observation_time=datetime.strptime(observation['time'], "%Y-%m-%dT%H:%M:%S+00:00")
                    last_synced_local_outside_weather = grootgle.last_sync_datas(localconn, "outside_weather",
                                                                                 "where name='{}'".format(station['name']))
                    if observation_time != last_synced_local_outside_weather:
                        grootgle.insert_outside_weather(localconn,station,observation)
                    else:
                        logging.debug("last observations already catched")

    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass

    # Close communication with the database.
    conn.close()
    localconn.close()

if __name__ == "__main__":
    main()