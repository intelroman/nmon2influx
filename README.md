# nmon2influx

This script:
1. Convert nmon format to csv
2. Convert csv files in json files formatted for influxdb feed.
3. Read json files and seend to influxdb.


# Collect 5 min data example: nmon -f -s 10 -c 30 -t
file format  <hostname>_<YYmmdd>_<HHMM>.nmon

- Put nmon/ files in nmon folder:

