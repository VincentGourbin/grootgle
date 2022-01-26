#!/usr/bin/env python3
import time
import random
import logging
from grootgle_tools import grootgle
from datetime import datetime

import psycopg2
from psycopg2.errors import SerializationFailure

def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    localconn = psycopg2.connect("dbname=aquarium user=pi")


    try:

        grootgle.run_transaction(conn, lambda conn: grootgle.sync_sensor_data(conn, localconn, grootgle.last_sync_datas(conn, "sensor")))
        grootgle.run_transaction(conn, lambda conn: grootgle.sync_objectives_data(conn, localconn, grootgle.last_sync_datas(conn, "objectives")))
        grootgle.run_transaction(conn, lambda conn: grootgle.sync_last_analysis_data(conn, localconn, grootgle.last_sync_datas(conn, "last_analysis")))
        grootgle.run_transaction(conn, lambda conn: grootgle.sync_last_ephemeride(conn, localconn, grootgle.last_sync_datas(conn, "ephemeride")))
        for row in grootgle.list_weather_station(localconn):
            grootgle.run_transaction(conn, lambda conn: grootgle.sync_last_outside_weather(conn,
                                                                                           localconn,
                                                                                           grootgle.last_sync_datas(conn, "outside_weather","where name='{}'".format(row[0])),
                                                                                           row[0]))
        #
        #run_transaction(conn, lambda conn: sync_last_outside_weather(conn, localconn, last_synced_local_outside_weather))

    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass

    # Close communication with the database.
    conn.close()

if __name__ == "__main__":
    main()
