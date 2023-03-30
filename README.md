# schneider-test

This repository contains a job for running daily reports on gharchive.org data containing github events.

To run the report, run the following command:
```
python daily_aggregation.py -y <Year> -m <Month> -d <Day>
```
It will generate two reports at *reports* folder.

Repositories report will be name as follows:
```
Repositories_report_<YearMonthDay>.json
```
Users report will be name as follows:
```
Repositories_report_<YearMonthDay>.json
```

If you don't have spark installed on your system, you can use the docker compose file to
run a spark master along with a worker.

To download monthly data of gharchive.org. You can use the following command:
```
python download_data.py -y <Year> -m <Month>
```
Output files will appear in the *data* folder, with the following name convention:
```
<Year>-<Month>-<Day>-<Hour>.json.gz
```