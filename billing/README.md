# Archive Tool
Due to there are over 50G new VOS and CDN billing logs created each day, we need to zip the files 2 days ago to avoid consuming the disk space too fast.

## deployment
1. Copy the archive.sh to /home/devops/billing/tools folder
2. Add cron job to run daily, edit /etc/cron.d/billing file
```
10 1 * * * root /home/devops/billing/tools/start.sh
```


# Statistic Tool
This tool is to analyze daily ImportLogService log and get the statistic metric by hour such as
- total files
- total records
- elapsed seconds

## deployment
1. Copy the statistic.py to the log file folder /data/logs/ImportLogService
2. run the following command, filename is like ImportLogService.2017-01-10.log
```
$ python statistic.py <filename>
```
The result is output as result.csv file, and the schema is
```
datetime, cdn file, cdn records, vos files, vos records
```


# Compare Tool
This tool is to compare the imported file numbers against files in specified folder for ImportLogService. It parse the ImportLogService.log and compare the data with actual file numbers to check if there is some files missing to imported to HBase.

## deployment
1. Copy the compare.py to the log file folder /data/logs/ImportLogService
2. run the following command, filename is like ImportLogService.2017-01-10.log
```
$ python compare.py <filename>
```


# DryRun Tool
This tool is to run ImportLogService one hour by hour. It differs from directly setting time across hours in ImportLogService that it won't aggregate all metric across hours and report to Grafana, in contrast, it report one hour by hour.

## deployment
1. Copy the dry_run.sh to the folder /data/ImportLogService
2. run the following command, date is like '2017-01-01'
```
$ ./dry_run.sh <date>
```

