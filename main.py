from pyhive import hive

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import hive_config as hive_config

# """
# yelp_academic_dataset_business
# yelp_academic_dataset_checkin
# yelp_academic_dataset_review
# yelp_academic_dataset_tip
# yelp_academic_dataset_user
# """
# df_business = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_business.json")
# df_checkin = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_checkin.json")
# df_review = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_review.json")
# df_tip = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_tip.json")
# df_user = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_user.json")

# df_business.printSchema()
# df_checkin.printSchema()
# df_review.printSchema()
# df_tip.printSchema()
# df_user.printSchema()

# df_business.limit(10).show()
# df_checkin.limit(10).show()
# df_review.limit(10).show()
# df_tip.limit(10).show()
# df_user.limit(10).show()

def spark_session():
    """
    Create spark session.

    :return:
    """
    spark = SparkSession.builder.getOrCreate()
    return spark

def load_datasets(spark):
    """
    load all datasets:
     - yelp_academic_dataset_business
     - yelp_academic_dataset_checkin
     - yelp_academic_dataset_review
     - yelp_academic_dataset_tip
     - yelp_academic_dataset_user

    :param spark:
    :return:
    """
    df_business = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_business.json")
    df_checkin = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_checkin.json")
    df_review = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_review.json")
    df_tip = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_tip.json")
    df_user = spark.read.json("D:\Haris\downloads\dataset\yelp\yelp_academic_dataset_user.json")

    return [df_business, df_checkin, df_review, df_tip, df_user]

def flatten_df(nested_df):
    """
    Flatten dataframe that contains struct data type.

    :param nested_df:
    :return:
    """
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def convert_json_to_csv(df,filename):
    """
    We can't convert to csv because df_business have struct data type. So we need to transform struct into array first
    so array can be explode (struct can't be explode). But the other problem is, when concat_ws, null value was
    removed whereas we need to keep the null values. So we need flatten_df to transform the dataframe without
    removing the null values.

    :param df:
    :param filename:
    :return:
    """
    if filename == 'business':
        # df.selectExpr("explode_outer(attributes) AS structCol").select(
        #     F.expr("concat_ws(',', structCol.*)").alias("attributes_str"))
        # df.selectExpr("explode_outer(attributes) AS structCol").selectExpr("structCol.*")
        #     .alias("attributes_str").show(truncate=False)
        df = flatten_df(df)
    df.printSchema()
    df.limit(10).show()
    df.coalesce(1).write.option("header", "true").csv("D:\Haris\downloads\dataset\yelp\converted_dataset\df_"+filename)

    return "df_" + filename + " converted into csv"

def transform_dataframe(df):
    """
    We need to drop all columns that doesn't necessary. Loop through all columns then drop the columns. After that, we
    need to add new column and define the conditions. Save the output to .csv file.

    :param df:
    :return:
    """
    df = df.drop(*[c for c in df.columns if c not in {'business_id', 'name', 'stars'}])
    df = df.withColumnRenamed("name","business_name")
    df = df.withColumn("conclusion", F.when((F.col("stars") == '5.0'), 'very recommend')
                       .when((F.col("stars") >= '4.0') & (F.col("stars") < '5.0'), 'recommend')
                       .when((F.col("stars") >= '3.0') & (F.col("stars") < '4.0'), 'average')
                       .otherwise('not recommend'))
    df.limit(10).show(truncate=False)
    df.coalesce(1).write.option("header", "true").csv(
        r"D:\Haris\downloads\dataset\yelp\transformed_dataset\df_business")
    return "df_business transformed and saved into csv"

def hive_conn():
    hive_conn = hive.Connection(host=hive_config['host_name'], port=hive_config['port'],
                             username=hive_config['username'], password=hive_config['password'],
                             database=hive_config['database'], auth='CUSTOM')
    return hive_conn

def create_external_table(hive):
    """
    Create external table in hive. First we need to store file on our bucket or on our hdfs. After that, we must define
    location to folder that our dataset stored like the query below.

    :param hive:
    :return:
    """
    query = (
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS trix__df_business 
        (
            business_id string,
            business_name string,
            stars double,
            conclusion string
        ) 
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'oss://ack-bucket/haris/datasets/yelp/transformed_dataset/df_business/'
        """
    )

    cursor = hive.cursor()

    try:
        cursor.execute(query)
        print(" ")
    except Exception as e:
        print("ERROR", str(e))
    cursor.close()
    return "table trix__df_business created"

if __name__ == '__main__':
    spark = spark_session()

    list_df = load_datasets(spark)
    list_filename = ['business', 'checkin', 'review', 'tip', 'user']
    for idx, item in enumerate(list_df):
        filename = list_filename[idx]
        convert_json_to_csv(item, filename)

    transform_dataframe(list_df[0])

    hive = hive_conn()
    create_external_table(hive)
