"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import numpy as np
import pandas as pd
import threading
from urllib.request import urlopen


def get_stations_from_networks():
    print('Getting stations from networks')

    """Build a station list by using a bunch of IEM networks."""
    stations = []
    lons = []
    lats = []
    states = """AK AL AR AZ CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME
     MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT
     WA WI WV WY"""

    # states = "CA"
    # IEM quirk to have Iowa AWOS sites in its own labeled network
    networks = []
    for state in states.split():
        networks.append("%s_ASOS" % (state,))

    for network in networks:
        # Get metadata
        uri = (
            "https://mesonet.agron.iastate.edu/geojson/network/%s.geojson"
        ) % (network,)
        data = urlopen(uri)
        jdict = json.load(data)
        for site in jdict["features"]:
            stations.append(site["properties"]["sid"])
            lons.append(site["geometry"]["coordinates"][0])
            lats.append(site["geometry"]["coordinates"][1])
    df = pd.DataFrame({'station':stations, 'lon':lons, 'lat':lats})
    df.to_csv('data/stations_US.csv')

    return stations


def download_data(uri):
    """Fetch the data from the IEM

    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.

    Args:
      uri (string): URL to fetch

    Returns:
      string data
    """
    attempt = 0
    while attempt < 6:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def fetch_data():

    stations = get_stations_from_networks()
    stations.sort()

    """Our main method"""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2022, 1, 15, 0, 0, 0)
    endts = datetime.datetime(2022, 1, 20, 0, 0, 0)

    SERVICE = "https://mesonet.agron.iastate.edu/request/asos/1min_dl.php?"
    service_part1 = SERVICE + "station%5B%5D="
    service_part2 = "&tz=UTC&" + startts.strftime("year1=%Y&month1=%m&day1=%d&hour1=%H&minute1=%M&") + \
                endts.strftime("year2=%Y&month2=%m&day2=%d&hour2=%H&minute2=%M") + \
                "&vars%5B%5D=pres1&sample=1min&what=view&delim=comma&gis=yes"

    # download data with multi-threading
    def worker(s):
        print("Downloading: %s" % (s,))

        uri = service_part1 + s + service_part2
        outfn = "data/raw_data/%s.txt" % (s)

        data = download_data(uri)
        out = open(outfn, "w")
        out.write(data)
        out.close()

    threads = []
    for station in stations:
        tmp = threading.Thread(target=worker, args=[station])
        threads.append(tmp)

    for thread in threads:
        thread.start()
        time.sleep(0.1)

    for thread in threads:
        thread.join()

    print('Finished downloading all data!')


if __name__ == "__main__":
    fetch_data()