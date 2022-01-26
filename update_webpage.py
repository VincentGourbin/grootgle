#!/usr/bin/env python3
from datetime import datetime
import time
import random
import logging
from grootgle_tools import grootgle
import psycopg2
from psycopg2.errors import SerializationFailure

localwebfile="/var/www/grootgle/index.html"

def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    localconn = psycopg2.connect("dbname=aquarium user=pi")

    try:
        last_synced_local_sensor_date=grootgle.last_sync_datas(localconn, "sensor").strftime("%d/%m/%y %H:%M:%S")
        last_synced_online_sensor_date = grootgle.last_sync_datas(conn, "sensor").strftime("%d/%m/%y %H:%M:%S")
        last_sensor_value = grootgle.get_last_sensor_data(localconn)
        avg_sensor_value = grootgle.get_avg_sensor_data(localconn,7)

        reading_file = open(localwebfile, "r")
        new_file_content = ""
        # reading file content line by line.
        lines = reading_file.readlines()
        # looping through each line in the file
        for line in lines:
            if "%LOCAL_PROBING_DATE%" in line:
                new_file_content += "<!--%LOCAL_PROBING_DATE%-->{}<!--%END_LOCAL_PROBING_DATE%-->\n".format(last_synced_local_sensor_date)
            elif "%ONLINE_PROBING_DATE%" in line:
                new_file_content += "<!--%ONLINE_PROBING_DATE%-->{}<!--%END_ONLINE_PROBING_DATE%-->\n".format(last_synced_online_sensor_date)
            #(datetime.datetime(2022, 1, 21, 12, 30, 18), Decimal('22.75'), 1345, 585, Decimal('24.30'), Decimal('40.64'), 24)
            elif "%LAST_SENSOR_VALUE%" in line:
                new_file_content += "<!--%LAST_SENSOR_VALUE%--><tr style='height: 76px;'><td class='u-border-1 " \
                                    "u-border-grey-30 u-first-column u-grey-5 u-table-cell u-table-cell-13'>Instant</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "</tr>" \
                                    "<!--%END_LAST_SENSOR_VALUE%-->\n".format(last_sensor_value[1],
                                                                                  last_sensor_value[2],
                                                                                  last_sensor_value[3],
                                                                                  last_sensor_value[4],
                                                                                  last_sensor_value[5])
            elif "%AVG_SENSOR_VALUE%" in line:
                new_file_content += "<!--%AVG_SENSOR_VALUE%--><tr style='height: 76px;'><td class='u-border-1 " \
                                    "u-border-grey-30 u-first-column u-grey-5 u-table-cell u-table-cell-13'>7 Days average</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.0f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.0f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "<td class='u-border-1 u-border-grey-30 u-table-cell'>{:.2f}</td>" \
                                    "</tr>" \
                                    "<!--%END_AVG_SENSOR_VALUE%-->\n".format(avg_sensor_value[1],
                                                                                  avg_sensor_value[2],
                                                                                  avg_sensor_value[3],
                                                                                  avg_sensor_value[4],
                                                                                  avg_sensor_value[5])
            else:
                new_file_content +=line
        reading_file.close()

        writing_file = open(localwebfile, "w")
        writing_file.write(new_file_content)
        writing_file.close()

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