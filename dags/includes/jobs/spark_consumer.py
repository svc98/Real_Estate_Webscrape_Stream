import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType


def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS real_estate
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

def create_table(session):
    session.execute("""
            CREATE TABLE IF NOT EXISTS real_estate.properties (
                APN TEXT,
                address TEXT,
                county TEXT,
                price INT,
                beds INT,
                baths INT,
                link TEXT,
                photos LIST<TEXT>,
                hoa_fee INT,
                buyers_agent_fee FLOAT,
                listing_agent_name TEXT,
                listing_agent_contact TEXT,
                redfin_agent_name TEXT,
                finished_sqft INT,
                unfinished_sqft INT,
                total_sqft INT,
                stories INT,
                style TEXT,
                year_built INT,
                year_renovated INT,
                PRIMARY KEY (APN, county)
                );
    """)

def create_cassandra_connection():
    session = Cluster(["cassandra"]).connect()
    if session is not None:
        create_keyspace(session)
        create_table(session)

    return session

def insert_data(session, **kwargs):
    print("Inserting data into Cassandra DB")
    session.execute("""
        INSERT INTO real_estate.properties(APN, address, county, price, beds, baths, link, photos, hoa_fee, buyers_agent_fee,
            listing_agent_name, listing_agent_contact, redfin_agent_name, finished_sqft, unfinished_sqft,
            total_sqft, stories, style, year_built, year_renovated)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, kwargs.values())



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    spark = (SparkSession.builder
                .appName('Redfin_Properties_Consumer')
                .config('spark.cassandra.connection.host', 'cassandra')
                .getOrCreate())

    kafka_df = (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "kafka:9092")
               .option("subscribe", "redfin_properties")
               .option('startingOffsets', 'earliest')                                                          # Will need to deduplicate
               .load())

    schema = StructType([
        StructField("address", StringType(), False),
        StructField("price", StringType(), False),
        StructField("beds", StringType(), False),
        StructField("baths", StringType(), False),
        StructField("link", StringType(), False),
        StructField("photos", ArrayType(StringType()), False),
        StructField("hoa_fee", StringType(), False),
        StructField("buyers_agent_fee", StringType(), False),
        StructField("listing_agent_name", StringType(), False),
        StructField("listing_agent_contact", StringType(), False),
        StructField("redfin_agent_name", StringType(), False),
        StructField("finished_sqft", StringType(), False),
        StructField("unfinished_sqft", StringType(), False),
        StructField("total_sqft", StringType(), False),
        StructField("stories", StringType(), False),
        StructField("style", StringType(), False),
        StructField("year_built", StringType(), False),
        StructField("year_renovated", StringType(), False),
        StructField("county", StringType(), False),
        StructField("APN", StringType(), False),
    ])

    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING)")
                    .select(from_json(col('value'), schema).alias('data'))
                    .select("data.*"))


    # Reorder Columns: APN + County
    cols = ["APN", "address", "county", "price", "beds", "baths", "link", "photos", "hoa_fee", "buyers_agent_fee",
            "listing_agent_name", "listing_agent_contact", "redfin_agent_name", "finished_sqft", "unfinished_sqft",
            "total_sqft", "stories", "style", "year_built", "year_renovated"]
    kafka_df = kafka_df.select(cols)

    kafka_df = (kafka_df.withColumn("price", kafka_df["price"].cast(IntegerType()))
                        .withColumn("beds", kafka_df["beds"].cast(IntegerType()))
                        .withColumn("baths", kafka_df["baths"].cast(IntegerType()))
                        .withColumn("hoa_fee", kafka_df["hoa_fee"].cast(IntegerType()))
                        .withColumn("buyers_agent_fee", kafka_df["buyers_agent_fee"].cast(FloatType()))
                        .withColumn("finished_sqft", kafka_df["finished_sqft"].cast(IntegerType()))
                        .withColumn("unfinished_sqft", kafka_df["unfinished_sqft"].cast(IntegerType()))
                        .withColumn("total_sqft", kafka_df["total_sqft"].cast(IntegerType()))
                        .withColumn("stories", kafka_df["stories"].cast(IntegerType()))
                        .withColumn("year_built", kafka_df["year_built"].cast(IntegerType()))
                        .withColumn("year_renovated", kafka_df["year_renovated"].cast(IntegerType())))


    query = (kafka_df.writeStream
                       .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
                            lambda row: insert_data(create_cassandra_connection(), **row.asDict())
                        ))
                       .start()
                       .awaitTermination())