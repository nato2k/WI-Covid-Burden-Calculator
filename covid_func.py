import pandas as pd
import urllib.request as ur
import json


# creates list of geoids from sql query
def creategeolist(geolist):
    geos = []
    i = 0
    for i in geolist:
        geos.append(i[0])
    return geos


# function to create Wisconsin Covid API URL based on GEOIDs
def createurl(geos):
    i = 0
    url = ""
    while i < len(geos) - 1:
        url = url + "GEOID%20%3D%20\'" + geos[i] + "\'%20OR%20"
        i = i + 1
    url = url + "GEOID%20%3D%20\'" + geos[i] + "'"
    return url


# setup zipcode api data zipcodes should be added to zips list ex zips = [ '12345' ] or zips = [ '12345', '54321' ]
# url for tract data https://www.huduser.gov/hudapi/public/usps?type=6&query=55089660100
# url for zipcode data request by zipcode https://www.huduser.gov/hudapi/public/usps?type=1&query=53092

# check if zip table exists

# def getzips(zips, tbl, typ, conn, c):
def getzips(zips, typ, zip_secret):
    zdf2 = None
    if typ == "zip":
        typn = "1"
    elif typ == "geo":
        typn = "6"
    else:
        print("please select zip or geo for typ in getzips()")
        exit()

    # values for typ must be 'geo' or 'zip'
    # c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='"+tbl+"'")
    # if c.fetchone()[0]==1 :
    #     print(tbl + " table exists dropping")
    #     c.execute("drop table " + tbl)
    if len(zips) > 1:
        z = 0
        while z < len(zips):
            print("Requesting data for: " + zips[z])
            zip_url = "https://www.huduser.gov/hudapi/public/usps?type=" + typn + "&query=" + zips[z]
            zreq = ur.Request(zip_url, data=None, headers={"Authorization": "Bearer " + zip_secret})
            zip_response = ur.urlopen(zreq)
            zdata = json.loads(zip_response.read())
            zdf = pd.DataFrame(zdata["data"])
            zdfs = zdf["results"].to_json(orient="values")
            zdfj = json.loads(zdfs)
            if z > 0:
                zdfnew = pd.DataFrame(zdfj)
                zdfnew[typ] = zips[z]
                zdf2 = pd.concat([zdf2, zdfnew], axis=0)
            else:
                zdf2 = pd.DataFrame(zdfj)
                zdf2[typ] = zips[z]
            z = z + 1
    else:
        print("Requesting data for: " + zips[0])
        zip_url = "https://www.huduser.gov/hudapi/public/usps?type=" + typn + "&query=" + zips[0]
        zreq = ur.Request(zip_url, data=None, headers={"Authorization": "Bearer " + zip_secret})
        zip_response = ur.urlopen(zreq)
        zdata = json.loads(zip_response.read())
        zdf = pd.DataFrame(zdata["data"])
        zdfs = zdf["results"].to_json(orient="values")
        zdfj = json.loads(zdfs)
        zdf2 = pd.DataFrame(zdfj)
        zdf2["Zip"] = zips[0]

    if typ == "geo":
        zdf2.rename({"geoid": "zip"}, axis='columns', inplace=True)
    return zdf2
