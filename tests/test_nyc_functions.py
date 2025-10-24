import pyspark.sql.connect.session

from src.nyctaxi_functions import *


def test_get_spark():
    spark = get_spark()
    assert isinstance(spark, pyspark.sql.connect.session.SparkSession)


def test_get_nyctaxi_trips():
    df = get_nyctaxi_trips()
    assert df.count() > 0
    df = get_nyctaxi_trips()
    assert df.count() > 0
