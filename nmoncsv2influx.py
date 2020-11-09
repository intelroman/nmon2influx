import csv, json, os, sys, subprocess, re, datetime, shutil
from influxdb import InfluxDBClient
from subprocess import PIPE
from pprint import pprint as pp
from sys import stdout
import pandas as pd
import conf as cfg
import logging

#Create csv files from nmon files.
for i in os.listdir(cfg.NMONF):
    if len(i.split("_")) >= 3 and re.match("\d{6}", i.split("_")[-2]) and re.match("\d{4}.nmon$", i.split("_")[-1]):
        hn = i.split('_')[0]
        try:
            subprocess.Popen(['pyNmonAnalyzer', '-x','-c', '-o', cfg.CSVF+'/'+hn, '-i', cfg.NMONF+'/'+i ], stdout=PIPE).wait()
        except:
            print ("pyNmonAnalyzer fail to process file {}".format(cfg.NMONF+'/'+i))
            os.rename(cfg.NMONF+"/"+i, cfg.FAILF+"/"+i+"_"+str(int(datetime.datetime.timestamp(datetime.datetime.now()))))
    elif i == ".gitkeep":
        pass
    else:
        print ("nmon file format not matching: host_YYmmdd_HHMM.nmon")
        os.rename(cfg.NMONF+"/"+i, cfg.FAILF+"/"+i+"_"+str(int(datetime.datetime.timestamp(datetime.datetime.now()))))

#Creating a list with the path of files in csv format.
csvfilePath = []
for dirName, subdirList, fileList in os.walk(cfg.CSVF, topdown=False):
    for fname in fileList:
        if re.match("[A-Z]+.csv$", fname):
            csvfilePath.append([re.search("/csv/(.*?)/csv", dirName).group(1), dirName+"/"+fname])

#Using pandas to conver csv to influxdb json
count = 0
for b,c in csvfilePath:
    obj = (c.split('/')[-1].strip('.csv'))
    try:
        data = pd.read_csv(c, index_col=0)
    except:
        print ("panas fail to read csv moving the file to bail folder for future investigation")
        os.rename(c, cfg.FAILF+"/"+c+str(int(datetime.datetime.timestamp(datetime.datetime.now()))))
    to_influx = []
    for i in data.keys()[0:]:
        for i1,i2 in enumerate(data[i]):
            tags = {"hostname": b, "monitored_obj": obj , "value": i}
            to_influx.append({"measurement":  "nmon",
                            "time": (datetime.datetime.strptime(data.index[i1], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')),
                            "tags": tags,
                            "fields": { "value" : i2 }
                            })
    with open(cfg.JSONF+"/"+b+"_"+obj+".json", 'w') as writeTOjson:
        json.dump(to_influx, writeTOjson)
    count += 1


#Creating list of json files for influxdb module.
jsonFilePath = []
for dirName, subdirList, fileList in os.walk(cfg.JSONF, topdown=False):
    for fname in fileList:
        if re.match(".*json$", fname):
            jsonFilePath.append(dirName+"/"+fname)

#connection to influx db
client = InfluxDBClient(host=cfg.influx['host'], username=cfg.influx['username'], port=cfg.influx['port'], password=cfg.influx['password'])
client.switch_database(cfg.influx['db'])

for i in jsonFilePath:
    try:
        client.write_points(json.load(open(i)))
    except:
        print("Error writing data from file: {}".format(i))
        os.rename(i, cfg.FAILF+"/"+i.split("/")[-1]+str(int(datetime.datetime.timestamp(datetime.datetime.now()))))

#At the end cleaup the folders
for i in os.listdir(cfg.CSVF):
    if i == ".gitkeep":
         pass
    else:
        shutil.rmtree(cfg.CSVF+"/"+i)