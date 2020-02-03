import sys

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from collections import defaultdict
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

    spark.sparkContext.setLogLevel("ERROR")
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option(
        "subscribe", "jira-event").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()

    df.printSchema()
    df.show()
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
