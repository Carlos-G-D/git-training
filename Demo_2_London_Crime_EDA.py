# To find out where pyspark is installed
import findspark
findspark.init()

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as func
import matplotlib.pyplot as plt

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local")\
            .appName("Analyzing London crime data")\
            .getOrCreate()

    sc = spark.sparkContext

    # Read csv as a Spark DataFrame
    data = spark.read\
                .format("csv")\
                .option("header", "true")\
                .load("../data/02/demos/datasets/london_crime_by_lsoa.csv")

    # print(type(data))
    data.printSchema()

    #print(data.count())

    #data.limit(5).show()

    # DATA CLEANING & SIMPLE EXPLORATION AND ANALYSIS
    # Erase the rows with missing data
    data.dropna()

    # Drop columns that are useless for the analysis to alleviate the computational resources of processing a huge
    # dataset
    data = data.drop("lsoa_code")
    # data.show(5)

    # Get the unique values of a column
    total_boroughs = data.select('borough')\
                         .distinct()

    # total_boroughs.show()
    # print(total_boroughs.count())

    # Filtering the dataset based in the value of an attribute
    hackney_data = data.filter(data['borough'] == "Hackney")
    # hackney_data.show(5)

    data_2015_2016 = data.filter(data['year'].isin(["2015", "2016"]))
    # data_2015_2016.sample(fraction=0.1).show()

    data_2014_onwards = data.filter(data['year'] >= 2014)
    # data_2014_onwards.sample(fraction=0.1).show()



    # GROUPING, AGGREGATING AND ORDERING DATA
    borough_crime_count = data.groupBy('borough').count()
    # borough_crime_count.show(5)

    borough_conviction_sum = data.groupBy('borough').agg({"value": "sum"})\
                                 .withColumnRenamed("sum(value)", "convictions") # Renaming the aggregated column
    # borough_conviction_sum.show(5)

    total_borough_convictions = borough_conviction_sum.agg({"convictions": "sum"})
    # total_borough_convictions.show()

    total_convictions = total_borough_convictions.collect()[0][0]
    # print(total_convictions)

    borough_percentage_contribution = borough_conviction_sum.withColumn(
        "% contribution",
        func.round(borough_conviction_sum.convictions / total_convictions * 100, 2)
    )
    borough_percentage_contribution.printSchema()

    borough_percentage_contribution.orderBy(borough_percentage_contribution[2].desc())\
                                   .show(10)



    conviction_monthly = data.filter(data['year'] == 2014)\
                             .groupBy('month')\
                             .agg({"value": "sum"})\
                             .withColumnRenamed("sum(value)", "convictions")

    total_conviction_monthly = conviction_monthly.agg({"convictions": "sum"})\
                                                 .collect()[0][0]

    total_conviction_monthly = conviction_monthly.withColumn(
        "percent",
        func.round(conviction_monthly.convictions/total_conviction_monthly * 100, 2)
    )
    # print(total_conviction_monthly.columns)
    # total_conviction_monthly.orderBy(total_conviction_monthly.percent.desc()).show()

    # AGGREGATIONS AND VISUALIZATION
    crimes_category = data.groupBy('major_category').agg({"value": "sum"}).withColumnRenamed("sum(value)", "convictions")
    crimes_category.orderBy(crimes_category.convictions.desc()).show()

    year_df = data.select('year')
    year_df.agg({'year': 'min'}).show()
    year_df.agg({'year': 'max'}).show()
    year_df.describe().show()

    data.crosstab('borough', 'major_category')\
        .select('borough_major_category', 'Burglary', 'Drugs', 'Fraud or Forgery', 'Robbery')\
        .show()

    plt.style.use('ggplot')

    ### AQUI YA ME HE CANSADO DE COPIAR, PARA CUALQUIER COSA, EN LOS NOTEBOOKS ADJUNTOS ESTAN LOS TROZOS DE CODIGO
    print('The end.')
    # Stopping Spark-Session and Spark context
    sc.stop()
    spark.stop()