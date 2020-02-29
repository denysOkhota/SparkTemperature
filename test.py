from pip._vendor.pyparsing import col
from pyspark.sql.functions import month
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("SparkTemperature").getOrCreate()
data = spark.read.csv("temperature_cities_2.csv", inferSchema=True, header=True,
                      encoding='windows-1251')

months = data.groupBy(month(data.date).alias('month')).agg(F.min(data.temperature).alias('min'),
                                                           F.max(data.temperature).alias('max'),
                                                           F.mean(data.temperature).alias('mean')) \
    .sort('month')
months.show()
# months.toPandas().to_csv('exit_csv.csv')


second = data.groupBy(data.name, month(data.date).alias('month')).agg(F.min(data.temperature).alias('min'),
                                                                      F.max(data.temperature).alias('max'),
                                                                      F.mean(data.temperature).alias('mean'),
                                                                      F.count(data.name).alias('name_cnt'),
                                                                      F.count(month(data.date)).alias(
                                                                          'month_cnt')).where(
    F.col('month_cnt') > 1)
second.toPandas().to_csv('exit_csv.csv', mode='a')
third = data.join(months, month(data.date) == months.month).withColumn("diff_min",
                                                                       data.temperature - F.col('min')).withColumn(
    "diff_mean", data.temperature - F.col('mean')).withColumn("diff_max", data.temperature - F.col('max'))
third.drop('mean', 'min', 'max', 'month').toPandas().to_csv('exit_csv.csv', mode='a')

months_range = spark.read.csv("cities.csv", inferSchema=True, header=True)
months_range = third.join(months_range, third.name == months_range.names).drop('diff_min', 'diff_mean',
                                                                               'diff_max', 'names')
months_range.toPandas().to_csv('exit_csv.csv', mode='a')
