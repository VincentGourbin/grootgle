from datetime import datetime
import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.errors import SerializationFailure

class grootgle:

    #generic run transaction class on a database
    def run_transaction(conn, op, max_retries=3):
        """
        Execute the operation *op(conn)* retrying serialization failure.

        If the database returns an error asking to retry the transaction, retry it
        *max_retries* times before giving up (and propagate it).
        """
        # leaving this block the transaction will commit or rollback
        # (if leaving with an exception)
        with conn:
            for retry in range(1, max_retries + 1):
                try:
                    op(conn)
                    # If we reach this point, we were able to commit, so we break
                    # from the retry loop.
                    return

                except SerializationFailure as e:
                    # This is a retry error, so we roll back the current
                    # transaction and sleep for a bit before retrying. The
                    # sleep time increases for each failed transaction.
                    logging.debug("got error: %s", e)
                    conn.rollback()
                    logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                    sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
                    logging.debug("Sleeping %s seconds", sleep_ms)
                    time.sleep(sleep_ms)

                except psycopg2.Error as e:
                    logging.debug("got error: %s", e)
                    logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                    raise e

            raise ValueError(f"Transaction did not succeed after {max_retries} retries")

    #generic class to get the last data added to a table
    def last_sync_datas(conn,table,optionnal=""):
        with conn.cursor() as cur:
            query="SELECT MAX(sync_date) as sync_date FROM {} {}".format(table,optionnal)
            logging.debug("last_sync_datas query :{}".format(query))
            cur.execute(query)
            logging.debug("last_sync_datas() for table %s : status message: %s", table, cur.statusmessage)
            conn.commit()
            last_synced_date = cur.fetchone()[0]
            if last_synced_date is None:
                last_synced_date = datetime.strptime("01/01/2020", "%d/%m/%Y")
            logging.debug("last_sync_datas() for table %s : last_synced_date: %s", table, last_synced_date)
        return last_synced_date

    #insert an ephemeride data from a webservice response (json)
    def insert_ephemeride(conn, ephemeride):
        with conn.cursor() as cur:
            cur.execute("INSERT INTO ephemeride(sync_date, sunrise, sunset, duration_day, diff_duration_day, "
                        "moon_age, moon_phase) VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(
                ephemeride['ephemeride']['datetime'],
                ephemeride['ephemeride']['sunrise'],
                ephemeride['ephemeride']['sunset'],
                ephemeride['ephemeride']['duration_day'],
                ephemeride['ephemeride']['diff_duration_day'],
                ephemeride['ephemeride']['moon_age'],
                ephemeride['ephemeride']['moon_phase']))
        cur.close()
        conn.commit()

    #insert a sensor data
    def insert_sensor(conn, current_date, water_temperature, tds_value, lux_value, temperature, humidity, delta):
        with conn.cursor() as cur:
            cur = conn.cursor()
            query= "INSERT INTO sensor (sync_date, Water_temp, Tds, Lux, Room_Temp, Room_humidity, " \
                   "Days_since_last_analysis) VALUES ('{}',{:.2f}, {:.0f},{:.0f}," \
                   "{:.2f},{:.2f},{:.0f})".format(current_date.strftime("%Y-%m-%d %H:%M:%S"),
                                                  water_temperature,
                                                  tds_value,
                                                  lux_value,
                                                  temperature,
                                                  humidity,
                                                  delta.days)
            logging.debug("insert_sensor query {}".format(query))
            cur.execute(query)
        cur.close()
        conn.commit()

    # insert an analysis data
    def insert_analysis(conn, current_date, last_analysis_date, nitrate, nitrite, salinity, calcium, magnesium, silicate):
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM last_analysis WHERE analysis_date>='{}'".format(last_analysis_date.strftime("%Y-%m-%d")))
            if cur.rowcount <= 0:
                query = "INSERT INTO last_analysis (sync_date, analysis_date, nitrates," \
                        "nitrites, salinity, calcium, magnesium, silicate) " \
                        "VALUES ('{}','{}',{},{},{},{},{},{})".format(current_date.strftime("%Y-%m-%d %H:%M:%S"),
                                                                      last_analysis_date.strftime("%Y-%m-%d"),
                                                                      nitrate,
                                                                      nitrite,
                                                                      salinity,
                                                                      calcium,
                                                                      magnesium,
                                                                      silicate)
                logging.debug("insert_last_analysis query {}".format(query))
                cur.execute(query)
            else:
                logging.debug("Last analysis already catched")
            cur.close()
            conn.commit()

    # insert objectives data
    def insert_objectives(conn, days_since_last_analysis_min,days_since_last_analysis_max,
                          days_since_last_analysis_target,water_temp_min, water_temp_max,
                          water_temp_target, tds_min, tds_max,tds_target, lux_min, lux_max,
                          lux_target, room_temp_min,room_temp_max, room_temp_target,
                          room_humidity_min,room_humidity_max,room_humidity_target, nitrates_min,
                          nitrates_max, nitrates_target,nitrites_min, nitrites_max,nitrites_target, salinity_min,
                          salinity_max, salinity_target,calcium_min, calcium_max,calcium_target, magnesium_min,
                          magnesium_max, magnesium_target,silicate_min, silicate_max,silicate_target,current_date):
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM objectives WHERE days_since_last_analysis_min={} and days_since_last_analysis_max={} and days_since_last_analysis_target={} and "
                "water_temp_min={} and water_temp_max={} and water_temp_target={} and "
                "tds_min={} and tds_max={} and tds_target={} and "
                "lux_min={} and lux_max={} and lux_target={} and "
                "room_temp_min={} and room_temp_max={} and room_temp_target={} and "
                "room_humidity_min={} and room_humidity_max={} and room_humidity_target={} and "
                "nitrates_min={} and nitrates_max={} and nitrates_target={} and "
                "nitrites_min={} and nitrites_max={} and nitrites_target={} and "
                "salinity_min={} and salinity_max={} and salinity_target={} and "
                "calcium_min={} and calcium_max={} and calcium_target={} and "
                "magnesium_min={} and magnesium_max={} and magnesium_target={} and "
                "silicate_min={} and silicate_max={} and silicate_target={}".format(days_since_last_analysis_min,
                                                                                    days_since_last_analysis_max,
                                                                                    days_since_last_analysis_target,
                                                                                    water_temp_min, water_temp_max,
                                                                                    water_temp_target, tds_min, tds_max,
                                                                                    tds_target, lux_min, lux_max,
                                                                                    lux_target, room_temp_min,
                                                                                    room_temp_max, room_temp_target,
                                                                                    room_humidity_min,
                                                                                    room_humidity_max,
                                                                                    room_humidity_target, nitrates_min,
                                                                                    nitrates_max, nitrates_target,
                                                                                    nitrites_min, nitrites_max,
                                                                                    nitrites_target, salinity_min,
                                                                                    salinity_max, salinity_target,
                                                                                    calcium_min, calcium_max,
                                                                                    calcium_target, magnesium_min,
                                                                                    magnesium_max, magnesium_target,
                                                                                    silicate_min, silicate_max,
                                                                                    silicate_target))
            if cur.rowcount <= 0:
                query = "INSERT INTO objectives (days_since_last_analysis_min,days_since_last_analysis_max," \
                        "days_since_last_analysis_target,water_temp_min,water_temp_max,water_temp_target," \
                        "tds_min,tds_max,tds_target,lux_min,lux_max,lux_target,room_temp_min, room_temp_max, " \
                        "room_temp_target, room_humidity_min, room_humidity_max ,room_humidity_target," \
                        "nitrates_min, nitrates_max, nitrates_target, nitrites_min, nitrites_max, nitrites_target, " \
                        "salinity_min, salinity_max, salinity_target, calcium_min, calcium_max, calcium_target, " \
                        "magnesium_min, magnesium_max, magnesium_target, silicate_min, silicate_max, " \
                        "silicate_target, sync_date) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}," \
                        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}," \
                        "{},{},{},'{}')".format(days_since_last_analysis_min,
                                                days_since_last_analysis_max,
                                                days_since_last_analysis_target,
                                                water_temp_min, water_temp_max, water_temp_target,
                                                tds_min, tds_max, tds_target,
                                                lux_min,lux_max, lux_target,
                                                room_temp_min, room_temp_max, room_temp_target,
                                                room_humidity_min, room_humidity_max, room_humidity_target,
                                                nitrates_min, nitrates_max, nitrates_target,
                                                nitrites_min, nitrites_max, nitrites_target,
                                                salinity_min, salinity_max, salinity_target,
                                                calcium_min, calcium_max, calcium_target,
                                                magnesium_min, magnesium_max, magnesium_target,
                                                silicate_min, silicate_max, silicate_target,
                                                current_date.strftime("%Y-%m-%d %H:%M:%S"))
                logging.debug("insert_objectives query {}".format(query))
                cur.execute(query)
            else:
                logging.debug("Objectives already catched")
            cur.close()
            conn.commit()

    # insert fuel history data data
    def insert_fuel_price_history(conn,station_name,station_brand,cp,city,update_date,price_sp95,price_e10,
                                  price_e85, price_gazole, price_sp98, price_gplc, latitude, longitude):
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM fuel_price_history WHERE latitude={} "
                        "and longitude={} "
                        "and update_date='{}'".format(latitude,longitude,update_date.strftime("%Y-%m-%d %H:%M:%S")))
            if cur.rowcount <= 0:
                query = "INSERT INTO fuel_price_history (sync_date,station_name,station_brand,cp,city," \
                        "update_date,price_sp95,price_e10,price_e85,price_gazole,price_sp98,price_gplc," \
                        "latitude,longitude) " \
                        "VALUES ('{}','{}','{}','{}','{}','{}',{},{},{},{},{},{},{},{})".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    station_name,
                    station_brand,
                    cp,
                    city,
                    update_date.strftime("%Y-%m-%d %H:%M:%S"),
                    price_sp95,
                    price_e10,
                    price_e85,
                    price_gazole,
                    price_sp98,
                    price_gplc,
                    latitude,
                    longitude)
                logging.debug("insert_fuel_price_history query {}".format(query))
                cur.execute(query)
            else:
                logging.debug("Fuel data already catched")
            cur.close()
            conn.commit()

    # insert fuel history data data
    def insert_brent_spot_price(conn,rate_date,rate):
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM brent_spot_price WHERE rate_date='{}'".format(rate_date.strftime("%Y-%m-%d %H:%M:%S")))
            if cur.rowcount <= 0:
                query = "INSERT INTO brent_spot_price (sync_date,rate_date,rate) " \
                        "VALUES ('{}','{}',{})".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    rate_date.strftime("%Y-%m-%d %H:%M:%S"),
                    rate)
                logging.debug("insert_brent_spot_price query {}".format(query))
                cur.execute(query)
            else:
                logging.debug("history rate already catched")
            cur.close()
            conn.commit()

    #insert an outside weather data from a webservice response (json)
    def insert_outside_weather(conn, station, observation):
        if 'time' in observation:
            time = observation['time']
        else:
            time = 0
        if 'name' in station:
            station_name = station['name']
        else:
            station_name = 0
        if 'outside_temperature' in observation:
            outside_temperature=observation['outside_temperature']['value']
        else:
            outside_temperature = 0
        if 'humidity' in observation:
            humidity=observation['humidity']['value']
        else:
            humidity = 0
        if 'rainfall' in observation:
            rainfall=observation['rainfall']['value']
        else:
            rainfall = 0
        if 'atmospheric_pressure' in observation:
            atmospheric_pressure=observation['atmospheric_pressure']['value']
        else:
            atmospheric_pressure = 0
        if 'global_radiation' in observation:
            global_radiation=observation['global_radiation']['value']
        else:
            global_radiation = 0
        if 'windgust_10m' in observation:
            windgust_10m=observation['windgust_10m']['value']
        else:
            windgust_10m = 0
        if 'wind_direction' in observation:
            wind_direction=observation['wind_direction']['value']
        else:
            wind_direction = 0
        if 'wind_10m' in observation:
            wind_10m=observation['wind_10m']['value']
        else:
            wind_10m = 0
        if 'insolation_time' in observation:
            insolation_time=observation['insolation_time']['value']
        else:
            insolation_time = 0
        if 'evapotranspiration' in observation:
            evapotranspiration=observation['evapotranspiration']['value']
        else:
            evapotranspiration = 0
        if 'wind_s' in observation:
            wind_s=observation['wind_s']['value']
        else:
            wind_s = 0
        if 'wind_direction_s' in observation:
            wind_direction_s=observation['wind_direction_s']['value']
        else:
            wind_direction_s = 0


        query="INSERT INTO outside_weather(sync_date,name,temperature,humidity,rainfall,atmospheric_pressure," \
              "global_radiation,windgust_10m,wind_direction,wind_10m,insolation_time,evapotranspiration,wind_s," \
              "wind_direction_s) VALUES ('{}','{}',{},{},{},{},{},{},{},{},{},{},{},{})".format(
                time,
                station_name,
                outside_temperature,
                humidity,
                rainfall,
                atmospheric_pressure,
                global_radiation,
                windgust_10m,
                wind_direction,
                wind_10m,
                insolation_time,
                evapotranspiration,
                wind_s,
                wind_direction_s)
        with conn.cursor() as cur:
            cur.execute(query)
        cur.close()
        conn.commit()

    #synchronise local and distant sensor data
    def sync_sensor_data(conn, localconn, last_synced_sensor_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            local_cur.execute(
                "SELECT sync_date, Water_temp, Tds, Lux, Room_Temp, Room_humidity, Days_since_last_analysis "
                "FROM sensor WHERE sync_date>'{}'".format(last_synced_sensor_date))
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                cur.execute("INSERT INTO sensor (sync_date, Water_temp, Tds, Lux, "
                            "Room_Temp, Room_humidity, Days_since_last_analysis) "
                            "VALUES ('{}',{:.2f}, {:.0f},{:.0f},{:.2f},{:.2f},{:.0f})".format(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
                logging.debug("sync_sensor_data() for row %s", row)

    # synchronise local and distant analysis data
    def sync_last_analysis_data(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            local_cur.execute(
                "SELECT sync_date, analysis_date, nitrates, nitrites, salinity, calcium, magnesium, silicate "
                "FROM last_analysis WHERE sync_date>'{}'".format(last_synced_date))
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                cur.execute("INSERT INTO last_analysis (sync_date, analysis_date, nitrates, nitrites, "
                            "salinity, calcium, magnesium, silicate) "
                            "VALUES ('{}','{}',{},{},{},{},{},{})".format(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
                logging.debug("sync_last_analysis_data() for row %s", row)

    # synchronise local and distant ephemeride data
    def sync_last_ephemeride(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            local_cur.execute(
                "SELECT sync_date, sunrise, sunset, duration_day, diff_duration_day, moon_age, moon_phase "
                "FROM ephemeride WHERE sync_date>'{}'".format(last_synced_date))
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                cur.execute(
                    "INSERT INTO ephemeride(sync_date, sunrise, sunset, duration_day, diff_duration_day, moon_age, moon_phase) "
                    "VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
                logging.debug("sync_last_ephemeride() for row %s", row)

    # synchronise local and distant objectives data
    def sync_objectives_data(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            local_cur.execute(
                "SELECT days_since_last_analysis_min,days_since_last_analysis_max,days_since_last_analysis_target, "
                "water_temp_min,water_temp_max,water_temp_target,tds_min,tds_max,tds_target,lux_min,lux_max,lux_target, "
                "room_temp_min, room_temp_max, room_temp_target, room_humidity_min, room_humidity_max ,room_humidity_target,"
                "nitrates_min, nitrates_max, nitrates_target,"
                "nitrites_min, nitrites_max, nitrites_target, salinity_min, salinity_max, salinity_target,"
                "calcium_min, calcium_max, calcium_target, magnesium_min, magnesium_max, magnesium_target,"
                "silicate_min, silicate_max, silicate_target, sync_date "
                "FROM objectives WHERE sync_date>'{}'".format(last_synced_date))
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                cur.execute("INSERT INTO objectives (days_since_last_analysis_min,days_since_last_analysis_max,"
                            "days_since_last_analysis_target,water_temp_min,water_temp_max,water_temp_target,"
                            "tds_min,tds_max,tds_target,lux_min,lux_max,lux_target,room_temp_min, room_temp_max, "
                            "room_temp_target, room_humidity_min, room_humidity_max ,room_humidity_target,nitrates_min, "
                            "nitrates_max, nitrates_target,nitrites_min, nitrites_max, nitrites_target, salinity_min, "
                            "salinity_max, salinity_target,calcium_min, calcium_max, calcium_target, magnesium_min, "
                            "magnesium_max, magnesium_target, silicate_min, silicate_max, silicate_target, sync_date) "
                            "VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},"
                            "{},{},{},{},{},{},{},{},{},{},{},{},{},'{}')".format(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],
                    row[12], row[13],
                    row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24],
                    row[25],
                    row[26], row[27], row[28], row[29], row[30], row[31], row[32], row[33], row[34], row[35], row[36]))
                logging.debug("sync_objectives_data() for row %s", row)

    # synchronise local and distant outside weather data per station
    def sync_last_outside_weather(conn, localconn, last_synced_date, station):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            logging.debug("sync_last_outside_weather() for station %s", station)
            query="SELECT sync_date,name,temperature,humidity,rainfall,atmospheric_pressure,global_radiation," \
                  "windgust_10m,wind_direction,wind_10m,insolation_time,evapotranspiration," \
                  "wind_s,wind_direction_s FROM outside_weather WHERE sync_date>'{}' and name='{}'".format(last_synced_date,station)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                logging.debug("sync_last_outside_weather() for row %s", row)
                cur.execute(
                    "INSERT INTO outside_weather(sync_date,name,temperature,humidity,rainfall,atmospheric_pressure,"
                    "global_radiation,windgust_10m,wind_direction,wind_10m,insolation_time,evapotranspiration,"
                    "wind_s,wind_direction_s) VALUES ('{}','{}',{},{},{},{},{},{},{},{},{},{},{},{})".format(
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                        row[11], row[12], row[13]))

    # synchronise local and distant outside weather data per station
    def sync_last_outside_weather(conn, localconn, last_synced_date, station):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            logging.debug("sync_last_outside_weather() for station %s", station)
            query="SELECT sync_date,name,temperature,humidity,rainfall,atmospheric_pressure,global_radiation," \
                  "windgust_10m,wind_direction,wind_10m,insolation_time,evapotranspiration," \
                  "wind_s,wind_direction_s FROM outside_weather WHERE sync_date>'{}' and name='{}'".format(last_synced_date,station)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                logging.debug("sync_last_outside_weather() for row %s", row)
                cur.execute(
                    "INSERT INTO outside_weather(sync_date,name,temperature,humidity,rainfall,atmospheric_pressure,"
                    "global_radiation,windgust_10m,wind_direction,wind_10m,insolation_time,evapotranspiration,"
                    "wind_s,wind_direction_s) VALUES ('{}','{}',{},{},{},{},{},{},{},{},{},{},{},{})".format(
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                        row[11], row[12], row[13]))

    # synchronise local and distant fuel price history
    def sync_fuel_price_history(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            query="SELECT sync_date,station_name,station_brand,update_date,price_sp95,price_e10,price_e85," \
                  "price_gazole,price_sp98,price_gplc,latitude,longitude,cp,city FROM fuel_price_history " \
                  "WHERE sync_date>'{}'".format(last_synced_date)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                logging.debug("sync_fuel_price_history() for row %s", row)

                if row[4] is None:
                    price_sp95="null"
                else:
                    price_sp95 = row[4]
                if row[5] is None:
                    price_e10="null"
                else:
                    price_e10 = row[5]
                if row[6] is None:
                    price_e85="null"
                else:
                    price_e85 = row[6]
                if row[7] is None:
                    price_gazole="null"
                else:
                    price_gazole = row[7]
                if row[8] is None:
                    price_sp98="null"
                else:
                    price_sp98 = row[8]
                if row[9] is None:
                    price_gplc="null"
                else:
                    price_gplc = row[9]

                cur.execute(
                    "INSERT INTO fuel_price_history(sync_date,station_name,station_brand,update_date,price_sp95,"
                    "price_e10,price_e85,price_gazole,price_sp98,price_gplc,latitude,longitude,cp,city) "
                    "VALUES ('{}','{}','{}','{}',{},{},{},{},{},{},{},{},'{}','{}')".format(row[0], row[1], row[2], row[3],
                                                                                  price_sp95, price_e10,price_e85,
                                                                                  price_gazole,price_sp98,price_gplc,
                                                                                  row[10],row[11],row[12],row[13]))

    # synchronise local and distant brent_spot_price
    def sync_brent_spot_price(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            query="SELECT sync_date,rate_date,rate FROM brent_spot_price " \
                  "WHERE sync_date>'{}'".format(last_synced_date)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                logging.debug("sync_brent_spot_price() for row %s", row)
                cur.execute(
                    "INSERT INTO brent_spot_price(sync_date,rate_date,rate) "
                    "VALUES ('{}','{}',{})".format(row[0], row[1], row[2]))

    # get the weather station present in the dataset
    def list_weather_station(conn):
        with conn.cursor() as cur:
            query="SELECT name FROM outside_weather GROUP BY name"
            logging.debug("Get all station name :")
            cur.execute(query)
            rows=cur.fetchall()
            conn.commit()
        return rows

    # get the last sensor data
    def get_last_sensor_data(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT sync_date, Water_temp, Tds, Lux, Room_Temp, Room_humidity, Days_since_last_analysis "
                        "FROM sensor WHERE sync_date=(select MAX(sync_date) FROM sensor)")
            row = cur.fetchone()
            conn.commit()
        return row

    # get the average sensor data for a time period
    def get_avg_sensor_data(conn, days):
        with conn.cursor() as cur:
            cur.execute("SELECT max(sync_date) as sync_date, avg(Water_temp) as Water_temp, avg(Tds) as Tds, "
                        "avg(Lux) as Lux, avg(Room_Temp) as Room_Temp, avg(Room_humidity) as Room_humidity, "
                        "avg(Days_since_last_analysis) as Days_since_last_analysis "
                        "FROM sensor WHERE sync_date>(SELECT CURRENT_DATE - INTERVAL '{} day')".format(days))
            row = cur.fetchone()
            conn.commit()
        return row

    #generic class to parse a command line
    def parse_cmdline():
        parser = ArgumentParser(description=__doc__,formatter_class=RawTextHelpFormatter)
        parser.add_argument(
            "dsn",
            help="""\
    database connection string
    For cockroach demo, use
    'postgresql://<username>:<password>@<hostname>:<port>/bank?sslmode=require',
    with the username and password created in the demo cluster, and the hostname
    and port listed in the (sql/tcp) connection parameters of the demo cluster
    welcome message.

    For CockroachCloud Free, use
    'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.bank?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

    If you are using the connection string copied from the Console, your username,
    password, and cluster name will be pre-populated. Replace
    <your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
    Console.

    """
        )

        parser.add_argument("-v", "--verbose",action="store_true", help="print debug info")
        opt = parser.parse_args()
        return opt