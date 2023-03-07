import requests
from requests.auth import HTTPBasicAuth

from pyspark.sql import SparkSession

from pyspark.dbutils import DBUtils


def query_es_api(spark, end_point):
    dbutils = DBUtils(spark)

    ES_HOST = dbutils.secrets.get(scope='elasticsearch', key="HOST")
    ID = dbutils.secrets.get(scope='elasticsearch', key="ID")
    PW = dbutils.secrets.get(scope='elasticsearch', key="PASSWORD")

    auth = HTTPBasicAuth(ID, PW)

    url = ES_HOST + end_point
    headers = {'Content-Type': 'application/json'}
    return requests.get(url, auth=auth, verify=False, headers=headers)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .config('spark.sql.sources.partitionColumnTypeInference.enabled', 'false') \
        .getOrCreate()

    res = query_es_api(spark, '_cluster/health')
    print(res)
