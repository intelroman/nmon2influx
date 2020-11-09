import os

CSVF = str(os.getcwd())+'/csv'
JSONF = str(os.getcwd())+'/json'
NMONF = str(os.getcwd())+'/nmon'
FAILF = str(os.getcwd())+'/fail'

influx = {
        "host" : "fqdn/ip",
        "port": 8086,
        "db": "stats",
        "username": "admin",
        "password": "yourinfluxdbpass"
}