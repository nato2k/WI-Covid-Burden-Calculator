import pandas as pd
import json
import sqlite3
import datetime
import urllib.request as ur
import pathlib
import os
from covid_func import *
from secrets import *

# covid.db should be saved in current path/data
try:
    path = str(pathlib.Path(__file__).parent.absolute())
    db = path + "/data/covid.db"
    print("DB path is: " + db)
except:
    path = str(os.getcwd())
    db = path + "/data/covid.db"
    print("DB path is: " + db)

# connect to Sqlite DB
try:
    print("connecting to " + db)
    conn = sqlite3.connect(db)
except:
    print("unable to connect to: " + db + " please confirm the file exists and is placed in the right directory")

c = conn.cursor()

# zip codes to create burden for
zips = ['53092', '53097']

# get zipcode data from hud
zdf = getzips(zips, "zip", zip_secret())
zdf.to_sql("zips", conn, if_exists='replace')

# check if census table exists
c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='census' ''')

if c.fetchone()[0] == 1:
    print("census table exists, not requesting new data")
else:
    # pull census data for WI
    print("Requesting census data...")
    census_secret = census_secret()
    census_url = "https://api.census.gov/data/2010/acs/acs5/profile?get=NAME,DP02_0017E&for=tract:*&in=state:55&key=" + census_secret
    census_res = ur.urlopen(census_url)
    census_data = json.loads(census_res.read())
    census_df = pd.DataFrame(census_data[1:], columns=census_data[0]).rename(columns={"DP02_0017E": "Pop"})
    census_df.to_sql("census", conn)
    c.execute("update census set geoid = state || county || tract;")
    conn.commit()

# set todays date format
d1 = datetime.datetime.today()
d1 = d1.strftime("%Y-%m-%d")

# create list of geoids based on zip codes
geolist = c.execute("select distinct geoid from zips").fetchall()
geo = creategeolist(geolist)

# zip code reverse lookup from geoid to zip to find out burden share
zdf = getzips(geo, "geo", zip_secret())
zdf.to_sql("geos", conn, if_exists='replace')

# create API URL
url = createurl(geo)
apiurl = "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_WI/MapServer/13/query?where=" + url + "&outFields=*&outSR=4326&f=json"

# url = "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_WI/MapServer/13/query?where=GEOID%20%3D%20'55089660100'%20OR%20GEOID%20%3D%20'55089660201'%20OR%20GEOID%20%3D%20'55089660202'%20OR%20GEOID%20%3D%20'55089660301'%20OR%20GEOID%20%3D%20'55089660303'%20OR%20GEOID%20%3D%20'55089660304'&outFields=*&outSR=4326&f=json"

print("requesting covid data from WI DHS...")
# Open JSON data
response = ur.urlopen(apiurl)
data = json.loads(response.read())

print("received api response...")

# Create A DataFrame From the JSON Data
df = pd.DataFrame(data["features"])
datas = df["attributes"].to_json(orient="values")
dataj = json.loads(datas)
df2 = pd.DataFrame(dataj)

# load covid data to covid_geo
df2.to_sql("covid_geo", conn, if_exists='replace')

# update date format
c.execute("update covid_geo set date = date(round(date / 1000), 'unixepoch', 'localtime');")
conn.commit()

# sql query to calculate burden
sql = """select zip, round(total) as total, burden_mult as burden_multiplier, round(total * burden_mult, 0) as burden
    from (
        select zip, sum(cases) as total, burden_mult from (
            select t2.zip, t1.new * t2.share as cases, t2.burden_mult from (
                select geoid, maxpos - minpos as new from (
                    select t1.positive as maxpos, t2.positive as minpos, t1.geoid from
                        (select positive, geoid from covid_geo where date = date('now', 'localtime')) t1
                join
                    (select positive, geoid from covid_geo where date = date('now', 'localtime','-14 days')) t2
                        on t1.geoid = t2.geoid)) t1
            join
                (select zip, burden_mult, share, geo from (
                select t1.*, t2.total_pop, 100000 / t2.total_pop as burden_mult from (
                    select zip, geo, sum(zip_pop) as zip_pop, res_ratio as share from (
                        select distinct t1.*, t2.pop * t1.res_ratio as zip_pop from geos t1
                        join census t2 on t1.geo = t2.geoid)
                    group by zip, geo) t1
                join (
                    select zip, sum(zip_pop) as total_pop from (
                        select distinct t1.*, t2.pop * t1.res_ratio as zip_pop from geos t1
                        join census t2 on t1.geo = t2.geoid)
                        group by zip) t2
                on t1.zip = t2.zip)) t2
        on t1.geoid = t2.geo)
    group by zip)
where zip in ({seq})
group by zip;""".format(seq=','.join(['?'] * len(zips)))

# print new data including date
maxdt = pd.read_sql_query("select distinct max(date) as dt from covid_geo;", conn)
print("data for: " + maxdt["dt"][0])
res = pd.read_sql_query(sql, conn, params=zips)

if len(res) == 0:
    print("no new results returned, exiting...")
    conn.close
    quit()

res["date"] = maxdt["dt"][0]
print(res)

# check if census table exists
c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='covid_results' ''')
if c.fetchone()[0] == 0:
    res.to_sql("covid_results", conn)
    print("saving new results...")
else:
    res_sql = """SELECT date from covid_results where date = '{}' """.format(maxdt["dt"][0])
    res_df = pd.read_sql_query(res_sql, conn)
    if len(res_df) == 0:
        res.to_sql("covid_results", conn, if_exists='append')
        print("saving new results...")
    else:
        print("no new results, not saving...")

conn.close
print("done")
