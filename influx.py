#!/usr/bin/env python3
"""Demo file showing how to use the miflora library."""

import re
import requests
from btlewrap import available_backends, BluepyBackend, GatttoolBackend, PygattBackend, base

from miflora.miflora_poller import MiFloraPoller, \
    MI_CONDUCTIVITY, MI_MOISTURE, MI_LIGHT, MI_TEMPERATURE, MI_BATTERY

from config import *
import json
from influxdb import InfluxDBClient

try:
    db_client = InfluxDBClient(*influx_args)
except ValueError:
    print("InfluxDBClient init failed. Check config!")
backend = None
clear_hosts = []
json_filename = '.cached_data'
# Check if data was cached and load it
try:
    with open(json_filename,'r') as json_file:
        json_body = json.load(json_file)
except Exception as e:
    json_body = []

def valid_miflora_mac(mac, pat=re.compile(r"C4:7C:8D:[0-9A-F]{2}:[0-9A-F]{2}:[0-9A-F]{2}")):
    """Check for valid mac adresses."""
    if not pat.match(mac.upper()):
        raise TypeError('The MAC address "{}" seems to be in the wrong format'.format(mac))
    return mac


def poll(mac, hostname):
    """Poll data from the sensor."""
    global json_body
    global clear_history
    poller = MiFloraPoller(mac, backend)
    try:
        measurement = {
                "measurement": "monitor_reading",
                "tags": {
                    "monitor": hostname
                },
                "time": int(poller._fetch_device_time()[1]),
                "fields": {
                    "firmware": poller.firmware_version(),
                    "battery": poller.parameter_value(MI_BATTERY),
                    "temperature": poller.parameter_value(MI_TEMPERATURE),
                    "moisture": poller.parameter_value(MI_MOISTURE),
                    "light": poller.parameter_value(MI_LIGHT),
                    "conductivity": poller.parameter_value(MI_CONDUCTIVITY)
                }
        }
        json_body.append(measurement)
    except Exception as e:
        print(str(e))


def history(mac, hostname):
    """Read the history from the sensor."""
    global json_body
    global clear_hosts
    temp = []
    poller = MiFloraPoller(mac, backend)
    history_list = poller.fetch_history()
    for entry in history_list:
        measurement = {
            "measurement": "monitor_reading",
            "tags": {
                "monitor": hostname
            },
            "time": int(entry.wall_time.timestamp()),
            "fields": {
                "temperature": entry.temperature,
                "moisture": entry.moisture,
                "light": entry.light,
                "conductivity": entry.conductivity
            }
        }
        temp.append(measurement)
    if len(history_list) == len(temp) and not len(history_list) == 0:
        for item in temp:
            json_body.append(item)
        clear_hosts.append(hostname)


def clear_history(mac):
    """Clear the sensor history."""
    poller = MiFloraPoller(mac, backend)
    poller.clear_history()


def _get_backend(config_backend):
    """Extract the backend class from the command line arguments."""
    if config_backend == 'gatttool':
        backend = GatttoolBackend
    elif config_backend == 'bluepy':
        backend = BluepyBackend
    elif config_backend == 'pygatt':
        backend = PygattBackend
    else:
        raise Exception('unknown backend: {}'.format(config_backend))
    return backend


def main():
    """Main function.

    Check config and start pushing data to Influx
    """
    global backend
    global json_body
    global clear_hosts
    backend = _get_backend(miflora_backend)
    for hostname in to_scan:
        try:
            mac = valid_miflora_mac(devices[hostname])
            print("connecting: %s @ %s" %(hostname,mac))
            poll(mac, hostname)
            history(mac, hostname)
        except TypeError as type:
            print("Mac-Address not correct, please check it!")
        except base.BluetoothBackendException as blue:
            print("We have a Bluetooth issue, please check your device!")
        except BrokenPipeError as pipe:
            print("History Data is corrupted!")
        except Exception as ex:
            print(str(ex))

    try:
        database_error = True
        db_client.write_points(json_body, time_precision='s')
        database_error = False
        # Only if transfer of history to DB was successfully transmitted, delete the history of the sensors!
        print(clear_hosts)
        for hostname in clear_hosts:
            mac = valid_miflora_mac(devices[hostname])
            clear_history(mac)
    except requests.exceptions.ConnectionError as connection:
        print("Connection to InfluxDB failed!")
    except Exception as e:
        print("Houston, we have a serious problem!")

    # Cache data, if there was a problem with the Database connection
    if database_error:
        try:
            with open(json_filename,'w') as json_file:
                json.dump(json_body,json_file)
        except Exception as e:
            print("Sorry, we lost also the cached data!")


if __name__ == '__main__':
    main()
