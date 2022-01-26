
import time
import board
import adafruit_dht
import busio
import adafruit_ads1x15.ads1015 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from DS18B20classFile import DS18B20
from datetime import datetime
from grootgle_tools import grootgle
from datetime import datetime
import logging

import psycopg2
from psycopg2.errors import SerializationFailure

#Parameter to modify
####### BEGIN #######
dht_pin=board.D26
file_input = "/home/pi/Scripts/analysis.txt"
file_input_objective = "/home/pi/Scripts/objectives.txt"
ads1115_probe_for_lux = ADS.P0
ads1115_probe_for_tds = ADS.P1
ds18b20_probe = 0
####### END #######

# function to read a sample of data and average it
def sampleread(gain_to_apply, chan_to_read, sample_point, sleep_time):
    # Create the I2C bus
    i2c = busio.I2C(board.SCL, board.SDA)
    # Create the ADC object using the I2C bus
    ads = ADS.ADS1015(i2c)
    # Set the gain to 2/3 seems to be the correct value according to cqrobot
    ads.gain = gain_to_apply
    # Create single-ended input on channel 0 (TDS Meter)
    chan = AnalogIn(ads, chan_to_read)
    # initialise the loop
    i = 1
    correct_read = 0
    voltage = 0
    # loop for sampling the results
    for i in range(sample_point):
        try:
            voltage = chan.voltage + voltage
            correct_read = correct_read + 1
        except RuntimeError as e:
            continue
        time.sleep(sleep_time)
    # average voltage read
    return voltage / correct_read



def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    localconn = psycopg2.connect("dbname=aquarium user=pi")

    # FIRST PART READ THE DHT DATA FOR THE ROOM CONDITION
    # Initial the dht device, with data pin connected to:
    dht = adafruit_dht.DHT22(board.D26)
    i = 1
    temperature = 0
    humidity = 0
    number_of_correct_read = 0

    try:
        for i in range(10):
            try:
                temperature = temperature + dht.temperature
                humidity = humidity + dht.humidity
                number_of_correct_read = number_of_correct_read + 1
            except RuntimeError as e:
                # Reading doesn't always work! Just print error and we'll try again
                print("Reading from DHT failure: ", e.args)
                continue
            time.sleep(1.0)
        temperature = temperature / number_of_correct_read
        humidity = humidity / number_of_correct_read
        # Then read the other probes
        # Sample point for average
        sample_point = 30
        # Sleep between sample
        sleep_time = 40 / 1000

        # *************** READ AND CALCULATE THE Temp the first probe************
        # read the current_temp for calculation
        devices = DS18B20()
        container = devices.tempC(ds18b20_probe)
        water_temperature = container

        # *************** READ AND CALCULATE THE TDS VALUE ON PIN0***************
        # calculate the coefficient for conversion
        ref_temperature = 25.0
        compensation_coefficient = 1.0 + 0.02 * (water_temperature - ref_temperature)
        # calculate the compensated voltage
        gain = 2 / 3
        compensated_voltage = sampleread(gain, ads1115_probe_for_tds,sample_point,sleep_time) / compensation_coefficient
        # calculate the TDS value
        tds_value = (133.42 * compensated_voltage * compensated_voltage * compensated_voltage
                     - 255.86 * compensated_voltage * compensated_voltage + 857.39 * compensated_voltage) * 0.5

        # *************** READ AND CALCULATE THE LUX VALUE ON PIN1***************
        gain = 2 / 3
        lux_value = (136 * sampleread(gain, ads1115_probe_for_lux,sample_point,sleep_time)) / 0.84

        # Read the last analysis
        file_read = open(file_input, "r")
        # reading file content line by line.
        lines = file_read.readlines()
        # looping through each line in the file
        for line in lines:
            if "Date" in line:
                last_analysis_date = datetime.strptime(line.strip("\n")[-10:], "%d/%m/%Y")
            if "Salinité" in line:
                salinity = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "Nitrates" in line:
                nitrate = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "Nitrites" in line:
                nitrite = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "Calcium" in line:
                calcium = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "Magnésium" in line:
                magnesium = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "Silicate" in line:
                silicate = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
        file_read.close()

        # Read the objectives analysis
        file_read = open(file_input_objective, "r")
        # reading file content line by line.
        lines = file_read.readlines()
        # looping through each line in the file
        for line in lines:
            if "days_since_last_analysis_min" in line:
                days_since_last_analysis_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "days_since_last_analysis_max" in line:
                days_since_last_analysis_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "days_since_last_analysis_target" in line:
                days_since_last_analysis_target = line.strip("\n").split(' ')[
                    line.strip("\n").split(' ').index(':') + 1]
            if "water_temp_min" in line:
                water_temp_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "water_temp_max" in line:
                water_temp_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "water_temp_target" in line:
                water_temp_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "tds_min" in line:
                tds_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "tds_max" in line:
                tds_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "tds_target" in line:
                tds_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "lux_min" in line:
                lux_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "lux_max" in line:
                lux_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "lux_target" in line:
                lux_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_temp_min" in line:
                room_temp_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_temp_max" in line:
                room_temp_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_temp_target" in line:
                room_temp_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_humidity_min" in line:
                room_humidity_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_humidity_max" in line:
                room_humidity_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "room_humidity_target" in line:
                room_humidity_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrates_min" in line:
                nitrates_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrates_max" in line:
                nitrates_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrates_target" in line:
                nitrates_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrites_min" in line:
                nitrites_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrites_max" in line:
                nitrites_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "nitrites_target" in line:
                nitrites_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "salinity_min" in line:
                salinity_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "salinity_max" in line:
                salinity_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "salinity_target" in line:
                salinity_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "calcium_min" in line:
                calcium_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "calcium_max" in line:
                calcium_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "calcium_target" in line:
                calcium_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "magnesium_min" in line:
                magnesium_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "magnesium_max" in line:
                magnesium_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "magnesium_target" in line:
                magnesium_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "silicate_min" in line:
                silicate_min = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "silicate_max" in line:
                silicate_max = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
            if "silicate_target" in line:
                silicate_target = line.strip("\n").split(' ')[line.strip("\n").split(' ').index(':') + 1]
        file_read.close()

        # get current date
        current_date = datetime.now()
        # Difference between two dates
        delta = current_date - last_analysis_date

        # Apply the read values to the database
        grootgle.run_transaction(conn, lambda conn: grootgle.insert_sensor(localconn,
                                                                          current_date,
                                                                          water_temperature,
                                                                          tds_value,
                                                                          lux_value,
                                                                          temperature,
                                                                          humidity,
                                                                          delta))
        grootgle.run_transaction(conn, lambda conn: grootgle.insert_analysis(localconn,
                                                                             current_date,
                                                                             last_analysis_date,
                                                                             nitrate,
                                                                             nitrite,
                                                                             salinity,
                                                                             calcium,
                                                                             magnesium,
                                                                             silicate))

        grootgle.run_transaction(conn, lambda conn: grootgle.insert_objectives(localconn,
                                                                               days_since_last_analysis_min,
                                                                               days_since_last_analysis_max,
                                                                               days_since_last_analysis_target,
                                                                               water_temp_min,
                                                                               water_temp_max,
                                                                               water_temp_target,
                                                                               tds_min,
                                                                               tds_max,
                                                                               tds_target,
                                                                               lux_min,
                                                                               lux_max,
                                                                               lux_target,
                                                                               room_temp_min,
                                                                               room_temp_max,
                                                                               room_temp_target,
                                                                               room_humidity_min,
                                                                               room_humidity_max,
                                                                               room_humidity_target,
                                                                               nitrates_min,
                                                                               nitrates_max,
                                                                               nitrates_target,
                                                                               nitrites_min,
                                                                               nitrites_max,
                                                                               nitrites_target,
                                                                               salinity_min,
                                                                               salinity_max,
                                                                               salinity_target,
                                                                               calcium_min,
                                                                               calcium_max,
                                                                               calcium_target,
                                                                               magnesium_min,
                                                                               magnesium_max,
                                                                               magnesium_target,
                                                                               silicate_min,
                                                                               silicate_max,
                                                                               silicate_target,
                                                                               current_date))

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


