"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import numpy as np
import pandas as pd

# Python 2 and 3: alternative 4
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


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
    while attempt < MAX_ATTEMPTS:
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


def get_stations_from_filelist(filename):
    """Build a listing of stations from a simple file listing the stations.

    The file should simply have one station per line.
    """
    stations = []
    for line in open(filename):
        stations.append(line.strip())
    return stations


def get_stations_from_networks():
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



def get_stations_from_all_networks():
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    lons = []
    lats = []

    all_networks = ["AF__ASOS", "AL_ASOS", "AK_ASOS", "AL__ASOS", "CA_AB_ASOS", 
    "DZ__ASOS", "AS__ASOS", "AO__ASOS", "AI__ASOS", "AQ__ASOS", 
    "AG__ASOS", "AR__ASOS", "AZ_ASOS", "AR_ASOS", "AM__ASOS", 
    "AW__ASOS", "AU__ASOS", "AT__ASOS", "AZ__ASOS", "BS__ASOS", 
    "BH__ASOS", "BD__ASOS", "BB__ASOS", "BY__ASOS", "BE__ASOS", 
    "BZ__ASOS", "BJ__ASOS", "BM__ASOS", "BT__ASOS", "BO__ASOS", 
    "BA__ASOS", "BW__ASOS", "BR__ASOS", "CA_BC_ASOS", "IO__ASOS", 
    "VG__ASOS", "BG__ASOS", "BF__ASOS", "BI__ASOS", "CA_ASOS", 
    "KH__ASOS", "CM__ASOS", "CV__ASOS", "CF__ASOS", "TD__ASOS", 
    "CL__ASOS", "CN__ASOS", "CO__ASOS", "CO_ASOS", "KM__ASOS", 
    "CG__ASOS", "CT_ASOS", "CK__ASOS", "CR__ASOS", "HR__ASOS", 
    "CU__ASOS", "CY__ASOS", "CZ__ASOS", "DE_ASOS", "CD__ASOS", 
    "DK__ASOS", "DJ__ASOS", "DM__ASOS", "DO__ASOS", "EC__ASOS", 
    "EG__ASOS", "SV__ASOS", "GQ__ASOS", "EE__ASOS", "ET__ASOS", 
    "FK__ASOS", "FM__ASOS", "FJ__ASOS", "FI__ASOS", "FL_ASOS", 
    "FR__ASOS", "GF__ASOS", "PF__ASOS", "GA__ASOS", "GM__ASOS", 
    "GA_ASOS", "GE__ASOS", "DE__ASOS", "GH__ASOS", "GI__ASOS", 
    "KY__ASOS", "GB__ASOS", "GR__ASOS", "GL__ASOS", "GD__ASOS",
    "GU_ASOS", "GT__ASOS", "GN__ASOS", "GW__ASOS", "GY__ASOS", 
    "HT__ASOS", "HI_ASOS", "HN__ASOS", "HK__ASOS", "HU__ASOS", 
    "IS__ASOS", "ID_ASOS", "IL_ASOS", "IN__ASOS", "IN_ASOS", 
    "ID__ASOS", "IA_ASOS", "AWOS", "IR__ASOS", "IQ__ASOS", 
    "IE__ASOS", "IL__ASOS", "IT__ASOS", "CI__ASOS", "JM__ASOS", 
    "JP__ASOS", "JO__ASOS", "KS_ASOS", "KZ__ASOS", "KY_ASOS", 
    "KE__ASOS", "KI__ASOS", "KW__ASOS", "LA__ASOS", "LV__ASOS", 
    "LB__ASOS", "LS__ASOS", "LR__ASOS", "LY__ASOS", "LT__ASOS", 
    "LA_ASOS", "LU__ASOS", "MK__ASOS", "MG__ASOS", "ME_ASOS", 
    "MW__ASOS", "MY__ASOS", "MV__ASOS", "ML__ASOS", "CA_MB_ASOS", 
    "MH__ASOS", "MD_ASOS", "MA_ASOS", "MR__ASOS", "MU__ASOS", 
    "YT__ASOS", "MX__ASOS", "MI_ASOS", "MN_ASOS", "MS_ASOS", 
    "MO_ASOS", "MD__ASOS", "MC__ASOS", "MT_ASOS", "MA__ASOS", 
    "MZ__ASOS", "MM__ASOS", "NA__ASOS", "NE_ASOS", "NP__ASOS", 
    "AN__ASOS", "NL__ASOS", "NV_ASOS", "CA_NB_ASOS", "NC__ASOS", 
    "CA_NF_ASOS", "NH_ASOS", "NJ_ASOS", "NM_ASOS", "NY_ASOS", 
    "NF__ASOS", "NI__ASOS", "NE__ASOS", "NG__ASOS", "NC_ASOS", 
    "ND_ASOS", "MP__ASOS", "KP__ASOS", "CA_NT_ASOS", "NO__ASOS", 
    "CA_NS_ASOS", "CA_NU_ASOS", "OH_ASOS", "OK_ASOS", "OM__ASOS", 
    "CA_ON_ASOS", "OR_ASOS", "PK__ASOS", "PA__ASOS", "PG__ASOS", 
    "PY__ASOS", "PA_ASOS", "PE__ASOS", "PH__ASOS", "PN__ASOS", 
    "PL__ASOS", "PT__ASOS", "CA_PE_ASOS", "PR_ASOS", "QA__ASOS", 
    "CA_QC_ASOS", "RI_ASOS", "RO__ASOS", "RU__ASOS", "RW__ASOS", 
    "SH__ASOS", "KN__ASOS", "LC__ASOS", "VC__ASOS", "WS__ASOS", 
    "ST__ASOS", "CA_SK_ASOS", "SA__ASOS", "SN__ASOS", "RS__ASOS", 
    "SC__ASOS", "SL__ASOS", "SG__ASOS", "SK__ASOS", "SI__ASOS", 
    "SB__ASOS", "SO__ASOS", "ZA__ASOS", "SC_ASOS", "SD_ASOS", 
    "KR__ASOS", "ES__ASOS", "LK__ASOS", "SD__ASOS", "SR__ASOS", 
    "SZ__ASOS", "SE__ASOS", "CH__ASOS", "SY__ASOS", "TW__ASOS", 
    "TJ__ASOS", "TZ__ASOS", "TN_ASOS", "TX_ASOS", "TH__ASOS", 
    "TG__ASOS", "TO__ASOS", "TT__ASOS", "TU_ASOS", "TN__ASOS", 
    "TR__ASOS", "TM__ASOS", "UG__ASOS", "UA__ASOS", "AE__ASOS", 
    "UN__ASOS", "UY__ASOS", "UT_ASOS", "UZ__ASOS", "VU__ASOS", 
    "VE__ASOS", "VT_ASOS", "VN__ASOS", "VA_ASOS", "VI_ASOS", 
    "WA_ASOS", "WV_ASOS", "WI_ASOS", "WY_ASOS", "YE__ASOS", 
    "CA_YT_ASOS", "ZM__ASOS", "ZW__ASOS"]

    # all_networks = ['TO__ASOS'] # FIXME: comment out this
    for network in all_networks:
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

    # print(stations)
    # print(lons, lats)
    df = pd.DataFrame({'station':stations, 'lon':lons, 'lat':lats})
    df.to_csv('data/stations.csv')
    return stations



def download_alldata():
    """An alternative method that fetches all available data.

    Service supports up to 24 hours worth of data at a time."""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2022, 1, 15, 2, 0, 0)
    endts = datetime.datetime(2022, 1, 16, 0, 0, 0)
    interval = datetime.timedelta(hours=0,minutes=1) # FIXME: what interval to user?

    service = SERVICE + "data=mslp&tz=Etc/UTC&format=comma&latlon=no&"

    now = startts
    while now < endts:
        # print(now)
        thisurl = service
        thisurl += now.strftime("year1=%Y&month1=%m&day1=%d&hour1=%H&minute1=%M&")
        thisurl += (now + interval).strftime("year2=%Y&month2=%m&day2=%d&hour2=%H&minute2=%M&")

        print(thisurl)

        print("Downloading: %s" % (now,))
        data = download_data(thisurl)
        outfn = "data/%s.txt" % (now.strftime("%Y%m%d%H%M"),)
        # print('outfile:', outfn)
        with open(outfn, "w") as fh:
            fh.write(data)
        now += interval
        break

def main():

    """Our main method"""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2022, 1, 15, 0, 0, 0)
    endts = datetime.datetime(2022, 1, 16, 0, 0, 0)

# https://mesonet.agron.iastate.edu/request/asos/1min_dl.php?
# station%5B%5D=1V4
# &tz=UTC& year1=2022&month1=1&day1=1&hour1=0&minute1=0
# &year2=2022&month2=1&day2=2&hour2=0&minute2=0
# &vars%5B%5D=pres1&sample=1min&what=view&delim=comma&gis=yes


    SERVICE = "https://mesonet.agron.iastate.edu/request/asos/1min_dl.php?"
    service1 = SERVICE + "station%5B%5D="
    service2 = "&tz=UTC&" + startts.strftime("year1=%Y&month1=%m&day1=%d&hour1=%H&minute1=%M&")
    service2 += endts.strftime("year2=%Y&month2=%m&day2=%d&hour2=%H&minute2=%M")
    service2 += "&vars%5B%5D=pres1&sample=1min&what=view&delim=comma&gis=yes"

    stations = get_stations_from_networks()
    stations.sort()
    for station in stations:
        uri = service1 + station + service2
        print("Downloading: %s" % (station,))
        data = download_data(uri)
        outfn = "data/%s_%s_%s.txt" % (
            station,
            startts.strftime("%Y%m%d%H%M"),
            endts.strftime("%Y%m%d%H%M"),
        )
        out = open(outfn, "w")
        out.write(data)
        out.close()


if __name__ == "__main__":
    # download_alldata()
    main()
    # get_stations_from_all_networks()