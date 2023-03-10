# ztech-data-eng-project
Data Engineer Advanced Challenge for ZTech

Working off of: https://github.com/bees-bank/ztech-data-code-challenge/blob/main/Data%20Engineer%20Advanced/de_advanced.md

Outstanding Issues:
- I was unable to get Airflow to properly submit jobs to the Spark cluster. Moving my in-process Docker work into this repo
- Due to that, the DAG has not been fully tested and timestamp hasn't been passed in

Areas for Improvements: 
- Adhere to Pyspark best practices
- Figure out the best way to share a timestamp between jobs. Would it be XComm?
- Figure out if partitioning is warranted for this project moving forward
- Test cases

Visualizations Explainer:
- I took "Characters count participating in at least one event each year" to be the distinct count of character_ids over the years (An event does end in 2037)
- The data has the yearly breakdown for events for Top 10 Characters, but plotly has grouped the years into decades

Monitoring/Alerting for Pipeline:
- Add schema and status checks for the Marvel API requests
- Add time-based alerting to let the oncall know if the job has not completed on schedule
- Add data-quality checks to make sure row counts and other heuristics are within a set range
- Have alerts of different levels set up in Splunk and PagerDuty (or a more current iteration) if warranted

Docker Commands (WIP):
```
docker build --rm --force-rm -t local_airflow_spark .
docker tag local_airflow_spark local_airflow_spark:latest
docker-compose up airflow-init
docker-compose up
```