# schneider-test

This repository contains a job for running daily reports on gharchive.org data containing github events.

To run the report, run the following command:

python daily_aggregation.py -y <Year> -m <Month> -d <Day>

It will generate two reports at reports folder.

Repositories report will be name as follows: Repositories_report_<YearMonthDay>.json
Users report will be name as follows: Repositories_report_<YearMonthDay>.json
