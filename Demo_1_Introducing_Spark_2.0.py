# To find out where pyspark is installed
import findspark
findspark.init()

import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local")\
            .appName('Demo 1')\
            .getOrCreate()

    sc = spark.sparkContext

    simple_data = sc.parallelize([1, "Alice", 50])
    # print(simple_data.count())
    # print(simple_data.first())
    # print(simple_data.take(2))
    # print(simple_data.collect())
    # df = simple_data.toDF()  # In this case, the function will give an error since the data contained in the RDD
    #                          # is heterogeneous and has no schema.

    records = sc.parallelize([[1, "Alice", 50], [2, "Bob", 80]])
    # print(records.collect())
    # print(records.count())
    # print(records.first())
    # print(records.take(2))

    df = records.toDF()  # Now, since every record has the same number of fields and each field has the same data type
                         # it is possible to convert the RDD into a DataFrame. Spark infers the schema

    #df.show()  #Column names have been automatically generated and assigned

    # If you want to assign names, you have to build the RDD with Row objects
    data = sc.parallelize([Row(id=1, name="Alice", score=50),
                          Row(id=2, name="Bob", score=80),
                          Row(id=3, name="Charlee", score=75)])

    df = data.toDF()
    # df.show()

    # Another benefit of Row objects is that they can include complex types
    complex_data = sc.parallelize([Row(col_float=1.44,
                                       col_integer=10,
                                       col_string="John",
                                       col_boolean=True,
                                       col_list=[1, 2, 3])])
    complex_data_df = complex_data.toDF()
    # complex_data_df.show()

    more_complex_data = sc.parallelize([Row(col_list=[1, 2, 3],
                                            col_dict={"k1": 0},
                                            col_row=Row(a=10, b=20, c=30),
                                            col_time=datetime(2014, 8, 1, 14, 1, 5)),
                                        Row(col_list=[1, 2, 3, 4, 5],
                                            col_dict={"k1": 0, "k2": 1},
                                            col_row=Row(a=40, b=50, c=60),
                                            col_time=datetime(2014, 8, 2, 14, 1, 6)),
                                        Row(col_list=[1, 2, 3, 4, 5, 6, 7],
                                            col_dict={"k1": 0, "k2": 1, "k3": 2},
                                            col_row=Row(a=70, b=80, c=90),
                                            col_time=datetime(2014, 8, 3, 14, 1, 7))
                                        ])

    more_complex_data_df = more_complex_data.toDF()
    # more_complex_data_df.show()


    # Checking the SQL Context
    sqlCtxt = SQLContext(sc)

    df = sqlCtxt.range(5)
    # df.show()
    # print(df.count())

    data = [('Alice', 50),
            ('Bob', 80),
            ('Charlee', 75)]

    # With the SQLContext we can create dataframes directly from the data instead of having to instantiate the RDD
    # first
    # sqlCtxt.createDataFrame(data).show()  # Auto-generated names
    # sqlCtxt.createDataFrame(data, ['Name', 'Score']).show()  # Defined names

    more_complex_data = [Row(col_list=[1, 2, 3],
                             col_dict={"k1": 0},
                             col_row=Row(a=10, b=20, c=30),
                             col_time=datetime(2014, 8, 1, 14, 1, 5)),
                         Row(col_list=[1, 2, 3, 4, 5],
                             col_dict={"k1": 0, "k2": 1},
                             col_row=Row(a=40, b=50, c=60),
                             col_time=datetime(2014, 8, 2, 14, 1, 6)),
                         Row(col_list=[1, 2, 3, 4, 5, 6, 7],
                             col_dict={"k1": 0, "k2": 1, "k3": 2},
                             col_row=Row(a=70, b=80, c=90),
                             col_time=datetime(2014, 8, 3, 14, 1, 7))
                         ]

    # sqlCtxt.createDataFrame(more_complex_data).show()

    # If you want though, you can also create dataframes with the classic "RDD First" method
    data = sc.parallelize([Row(id=1, name="Alice", score=50),
                           Row(id=2, name="Bob", score=80),
                           Row(id=3, name="Charlee", score=75)])
    column_names = Row('id', 'name', 'score')
    students = data.map(lambda r: column_names(*r))

    students_df = sqlCtxt.createDataFrame(students)
    # students_df.show()

    # print(more_complex_data_df.first())

    # Access to individual elements of the dataframe in a classic matrix fashion.
    # It returns a copy of the content of the cell, so the original data in the dataframe is not modified
    cell_row = more_complex_data_df.collect()[0][2]
    # print(cell_row)

    cell_list = more_complex_data_df.collect()[0][0]
    cell_list.append(100)
    # print(cell_list)

    # Select a subset of columns
    # Option 1: Dataframe
    subset = more_complex_data_df.select('col_list', 'col_row')
    # subset.show()

    # Option 2: RDD
    # subset = more_complex_data_df.rdd\
    #                              .map(lambda x: (x.col_string, x.col_dictionary))\
    #                              .collect()

    # New columns can be derived with the "withColumn" method. Keep in mind this new column will only be present
    # in the new
    more_complex_data_df.select('col_list')\
                        .withColumn("col_sum", more_complex_data_df.col_list[0] + more_complex_data_df.col_list[1])\
                        .show()

    # We can also rename the columns of the dataframe. Again, those changes are not applied to the original dataframe,
    # instead, a new one is created.
    more_complex_data_df.withColumnRenamed('col_dict', 'col_map').show()


    # Finally, you can convert spark dataframes into pandas dataframes and viceversa
    # It is important to realize that spark dataframes are distributed, whereas the pandas dataframes are located
    # in memory in a single machine. So when converting from spark to pandas we lose the distributed category, and
    # when converting from pandas to spark we distribute the dataframe.
    df_pandas = more_complex_data_df.toPandas()
    print(df_pandas)
    print(type(df_pandas))

    df_spark = sqlCtxt.createDataFrame(df_pandas)
    df_spark.show()
    print(type(df_spark))

    # Stopping Spark-Session and Spark context
    sc.stop()
    spark.stop()