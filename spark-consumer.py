import sys

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, ArrayType, FloatType, StringType, StructField, StructType
from collections import defaultdict
import org.apache.spark.sql.functions._.
import time

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_to_kudu.py <kafka-brokers> <kudu-masters>")
        exit(-1)

    kuduTableName = "jira_events"
    kafkaBrokers, kuduMasters = sys.argv[1:]
    topicSet = ["jira-event"]

    spark = SparkSession.builder.appName("KafkaToKuduPython").getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 5)

    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("webhookEvent", StringType()) \
        .add("issue_event_type_name", StringType()) \
        .add("user", StructType()
             .add("self", StringType())
             .add("accountId", StringType())
             .add("displayName", StringType())
             .add("active", StringType())
             .add("accountType", StringType())) \
        .add("issue", StructType()
             .add("id", StringType())
             .add("self", StringType())
             .add("key", StringType())
             .add("fields", MapType(StringType(), StructType()
                                    .add("statuscategorychangedate", StringType())
                                    .add("issuetype", MapType(StringType(), StructType()
                                                              .add("self", StringType())
                                                              .add("id", StringType())
                                                              .add("description", StringType())
                                                              .add("name", StringType())
                                                              .add("subtask", StringType())))))
             .add("timespent", StringType())
             .add("project", MapType(StringType(), StructType()
                                    .add("self", StringType())
                                    .add("id", StringType())
                                    .add("key", StringType())
                                    .add("name", StringType())
                                    .add("projectTypeKey", StringType())))) \
             .add("fixVersions", ArrayType(StringType(), True), True) \
             .add("aggregatetimespent", StringType(), True) \
             .add("resolution", StringType(), True) \
             .add("resolutiondate", StringType(), True) \
             .add("created", StringType())

    nestTimestampFormat="yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    jsonOptions={"timestampFormat": nestTimestampFormat}

    spark.sparkContext.setLogLevel("ERROR")
    df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option(
        "subscribe", "jira-event").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()

    query=df.writeStream.outputMode("append").format("console").start()

    parsed=df.select(from_json(col("value").cast("string"),
                                 schema, jsonOptions).alias("parsed_value"))
    parsed.printSchema()
    df.printSchema()

    # spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master', kuduMasters)\
    #      .option('kudu.table', kuduTableName).load().registerTempTable(kuduTableName)
    # spark.sql("INSERT INTO TABLE {table} from (select uuid(), current_timestamp(), '{payload}')".format(
    #     table=kuduTableName, payload=row.value()))

    query.awaitTermination()

    # dstream = KafkaUtils.createDirectStream(
    #     ssc, topicSet, {"metadata.broker.list": kafkaBrokers})
    # windowedStream = dstream.window(60)

    # def debug(x):
    #     print("{}".format(x))

    # def process(time, rdd):
    #     if rdd.isEmpty() == False:
    #         collection = rdd.collect()
    #         result = list(zip(*collection))[1]
    #         a = "{}".format(result[0])
    #         # print(a)
    #         spark.read.json(a).show()
    #         # debug(result[0])
    #         spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master', kuduMasters)\
    #              .option('kudu.table', kuduTableName).load().registerTempTable(kuduTableName)

    #        # str = ''.join(collection[0][1])

    #         # df.show(truncate=False)
    #       #  df.printSchema()
    #        # df.show()
    #         spark.sql("INSERT INTO TABLE {table} from (select uuid(), current_timestamp(), '{payload}')".format(
    #             table=kuduTableName, payload=result[0]))

    # windowedStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
