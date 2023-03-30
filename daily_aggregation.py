import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark

YEAR = "2022"
MONTH = "01"
DAY = "01"


def cnt_cond(cond: bool) -> int:
    """This function ..."""
    return fn.sum(fn.when(cond, 1).otherwise(0))


def get_repo_report(df: pyspark.sql.dataframe, date: str) -> None:
    """
    This function takes a df and writes the Repository report file to the reports folder.
    """

    print("Repository report is now being generated...")

    repo_df = df.select(fn.col("repo.name").alias("repo_name"),
                        fn.col("repo.id").alias("repo_id"),
                        fn.col("actor.id").alias("user_id"),
                        fn.col("payload.action").alias("action"),
                        fn.to_date(fn.col("created_at")).alias("date"),
                        fn.col("type").alias("event_type")
                        )

    repo_agg_df = repo_df.groupBy('repo_id', 'date') \
        .agg(fn.countDistinct(fn.when(fn.col('event_type') == "WatchEvent", fn.col("user_id")).otherwise(None)).alias('UsersThatStarredNum'),  # Check
             fn.countDistinct(fn.when(fn.col('event_type') == "ForkEvent", fn.col(
                 "user_id")).otherwise(None)).alias('UsersThatForkedNum'),  # Check
             cnt_cond((fn.col('event_type') == "IssuesEvent") & (
                 fn.col('action') == 'opened')).alias('CreatedIssueEvent'),
             cnt_cond((fn.col('event_type') == "PullRequestEvent") &
                      (fn.col('action') == 'opened')).alias('PRNum'),
             fn.array_distinct(fn.collect_list(
                 fn.col("repo_name"))).alias("repo_names")
             )

    #repo_agg_df.coalesce(1).write.json(f"./reports/Repositories_report_{date}.json")
    repo_agg_df.toPandas().to_json(f"./reports/Repositories_report_{date}.json", orient='records', lines=True)

    print("Report generated successfully")

    return


def get_user_report(df: pyspark.sql.dataframe, date: str) -> None:
    """
    This function takes a df and writes the User report file to the reports folder.
    """

    print("User report is now being generated...")

    user_df = df.select(
        fn.to_date(fn.col("created_at")).alias("date"),
        fn.col("actor.id").alias("user_id"),
        fn.col("actor.login").alias("user_login"),
        fn.col("payload.action").alias("action"),
        fn.col("type").alias("event_type"),
        fn.col("repo.id").alias("repo_id")
    ).filter(fn.col("user_login").endswith('[bot]') == False)  # To avoid bots and only show users

    def cnt_cond(cond): return fn.sum(fn.when(cond, 1).otherwise(0))

    user_agg_df = user_df.groupBy('user_id') \
        .agg(
        fn.countDistinct(fn.when(fn.col('event_type') == "WatchEvent", fn.col(
            "repo_id")).otherwise(None)).alias('StarredReposNum'),  # Check
        cnt_cond((fn.col('event_type') == "IssuesEvent") & (
            fn.col('action') == 'opened')).alias('CreatedIssuesNum'),
        cnt_cond((fn.col('event_type') == "PullRequestEvent") &
                 (fn.col('action') == 'opened')).alias('PRNum'),
        fn.array_distinct(fn.collect_list(
            fn.col("user_login"))).alias("user_login")
    )

    #new_df.coalesce(1).write.json(f"./reports/Users_report_{date}.json")
    user_agg_df.toPandas().to_json(f"./reports/Users_report_{date}.json", orient='records', lines=True)

    print("Report generated successfully")

    return


def main(year: str = YEAR, month: str = MONTH, day: str = DAY) -> None:

    spark = SparkSession.builder.getOrCreate()
    print("Loading data...")
    df = spark.read.format('json').load(
        f'./data/{YEAR}-{MONTH}-{DAY}-*.json.gz')
    date = year+month+day
    get_repo_report(df, date)
    get_user_report(df, date)


if __name__ == '__main__':

    argParser = argparse.ArgumentParser()
    argParser.add_argument("-y", "--year", help="Year of the report")
    argParser.add_argument("-m", "--month", help="Month of the report")
    argParser.add_argument("-d", "--day", help="Day of the report")

    args = argParser.parse_args()

    main(args.year, args.month, args.day)
