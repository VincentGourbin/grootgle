from picamera import PiCamera, Color
from time import sleep
import RPi.GPIO as GPIO
from fractions import Fraction
import datetime as dt

# Set the input  name
file_input = "/home/pi/Scripts/analog.txt"
#
isoday=50
isonight=800

file_read = open(file_input, "r")

# reading file content line by line.
lines = file_read.readlines()
# looping through each line in the file
lux_value = lines[0].strip("\n").split(' ')[lines[0].strip("\n").split(' ').index('LUX:')+1]
file_read.close()

GPIO.setmode(GPIO.BCM) ## Use board pin numbering
GPIO.setup(19, GPIO.OUT) ## Setup GPIO Pin 12 to OUT

camera = PiCamera()
camera.resolution = (2592, 1944)
camera.framerate = 15
camera.start_preview()
sleep(5)

if int(lux_value) < 50:
    #switch on the flash for one minute
    GPIO.output(19,True) ## Turn on GPIO pin 7
    sleep(60)
    camera.exposure_mode = 'night'
    camera.iso = isonight
    camera.framerate = Fraction(1, 6)
    camera.shutter_speed = 6000000
    camera.exposure_mode = 'off'
else:
    camera.exposure_mode = 'beach'
    camera.iso = isoday

camera.annotate_background = Color('black')
camera.annotate_text = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

camera.capture('/home/pi/temp/image%s.jpg' % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
GPIO.output(19,False) ## Turn on GPIO pin 7

