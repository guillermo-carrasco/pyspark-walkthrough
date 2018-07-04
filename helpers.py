""" Helper methods for the Spark walkthrough
"""

def create_temp_tables(spark):
    """ Create some temporal tables for data manipulation

    Params:
        spark - Spark session
    """
    merchants = spark.read.csv('data/merchants.csv', header=True, inferSchema=True)
    products = spark.read.csv('data/products.csv', header=True, inferSchema=True)
    merchants.createOrReplaceTempView('merchants')
    products.createOrReplaceTempView('products')
