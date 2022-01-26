#!/usr/bin/python3
from contextlib import closing
from urllib.request import urlopen
import requests
import json
from datetime import datetime
import time
import random
import logging
import psycopg2
from psycopg2.errors import SerializationFailure
from grootgle_tools import grootgle
import xlrd




#Variables à changer
rows=10000
distance="48.7173646%2C+2.1039848%2C+100000"
local_db="dbname=fuel user=ubuntu"
url_for_brent_price="https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls"
url_for_full_price_list="https://public.opendatasoft.com/explore/dataset/prix_des_carburants_j_7/download/?format=json&timezone=Europe/Paris&lang=fr"
date_for_rate_limit=datetime.fromisoformat('2021-12-01')
####################

url_fuel=('https://public.opendatasoft.com/api/records/1.0/search/?dataset=prix_des_carburants_j_7'
          '&rows={}'
          '&geofilter.distance={}').format(rows,distance)

def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    localconn = psycopg2.connect(local_db)

    try:
        #grab the brent price evolution in xls format a parse it
        r = requests.get(url_for_brent_price, allow_redirects=True)
        open('temp.temp', 'wb').write(r.content)
        book = xlrd.open_workbook('temp.temp')
        sh = book.sheet_by_index(1)
        # history_list=[["date","valeur"]]
        for x in range(sh.nrows):
            try:
                date_rate = xlrd.xldate.xldate_as_datetime(int(sh.cell_value(rowx=x, colx=0)), 0)
                rate_in_usd = float(sh.cell_value(rowx=x, colx=1))
                if date_rate >= date_for_rate_limit:
                    grootgle.insert_brent_spot_price(localconn, date_rate, rate_in_usd)
                    # history_list.append([date_rate,rate_in_usd])
            except (ValueError, IndexError, xlrd.XLDateError) as err:
                logging.debug('Exception raised {}'.format(err))
                pass
        logging.debug('url_fuel {}'.format(url_fuel))

        r = requests.get(url_for_full_price_list, allow_redirects=True)
        open('temp.temp', 'wb').write(r.content)
        f = open('temp.temp','r')
        decoded = json.load(f)
        if decoded:
            for current in decoded:
                try:
                    (fields, geometry) = (current[k] for k in ('fields', 'geometry'))
                    if 'name' in fields:
                        station_name = fields['name'].replace("'", "''''")
                    else:
                        station_name = ""
                    if 'brand' in fields:
                        station_brand = fields['brand'].replace("'", "''''")
                    else:
                        station_brand = ""
                    if 'cp' in fields:
                        cp = fields['cp'].replace("'", "''''")
                    else:
                        cp = ""
                    if 'city' in fields:
                        city = fields['city'].replace("'", "''''")
                    else:
                        city = ""
                    if 'update' in fields:
                        try:
                            update_date = datetime.strptime(fields['update'][:19], "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            update_date = None
                    else:
                        update_date = None
                    if 'price_sp95' in fields:
                        price_sp95 = fields['price_sp95']
                    else:
                        price_sp95 = "null"
                    if 'price_e10' in fields:
                        price_e10 = fields['price_e10']
                    else:
                        price_e10 = "null"
                    if 'price_e85' in fields:
                        price_e85 = fields['price_e85']
                    else:
                        price_e85 = "null"
                    if 'price_gazole' in fields:
                        price_gazole = fields['price_gazole']
                    else:
                        price_gazole = "null"
                    if 'price_sp98' in fields:
                        price_sp98 = fields['price_sp98']
                    else:
                        price_sp98 = "null"
                    if 'price_gplc' in fields:
                        price_gplc = fields['price_gplc']
                    else:
                        price_gplc = "null"
                    if 'coordinates' in geometry:
                        latitude = geometry['coordinates'][1]
                        longitude = geometry['coordinates'][0]
                    else:
                        latitude = None
                        longitude = None

                    grootgle.insert_fuel_price_history(localconn, station_name, station_brand, cp, city, update_date,
                                                       price_sp95, price_e10, price_e85, price_gazole,
                                                       price_sp98, price_gplc, latitude, longitude)
                except (ValueError, KeyError) as err:
                    print(err)
                    pass



        grootgle.run_transaction(conn, lambda conn: grootgle.sync_fuel_price_history(conn,localconn,grootgle.last_sync_datas(conn, "fuel_price_history")))
        grootgle.run_transaction(conn, lambda conn: grootgle.sync_brent_spot_price(conn,localconn,grootgle.last_sync_datas(conn, "brent_spot_price")))

    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)

    # Close communication with the database.
    conn.close()
    localconn.close()

if __name__ == "__main__":
    main()