#!/bin/bash
python3 /home/pi/Scripts/read_and_save_data.py ""
python3 /home/pi/Scripts/sync_online_database.py "postgresql://vincentgourbin:uYjz-PplBcO2yWla@free-tier5.gcp-europe-west1.cockroachlabs.cloud:26257/aquarium?sslmode=verify-full&sslrootcert=$HOME/.postgresql/root.crt&options=--cluster%3Dgrootgle-aquarium-2692"
python3 /home/pi/Scripts/update_webpage.py "postgresql://vincentgourbin:uYjz-PplBcO2yWla@free-tier5.gcp-europe-west1.cockroachlabs.cloud:26257/aquarium?sslmode=verify-full&sslrootcert=$HOME/.postgresql/root.crt&options=--cluster%3Dgrootgle-aquarium-2692"
python3 /home/pi/Scripts/update_meteo.py ""
