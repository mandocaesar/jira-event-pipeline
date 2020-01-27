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

    kuduTableName = "default.jira_events"
    kafkaBrokers, kuduMasters = sys.argv[1:]
    topicSet = ["jira-event"]

    spark = SparkSession.builder.appName("KafkaToKuduPython").getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 5)

    dstream = KafkaUtils.createDirectStream(
        ssc, topicSet, {"metadata.broker.list": kafkaBrokers})
    windowedStream = dstream.window(60)

    def process(time, rdd):
        # current date and time
        timestamp = time.time()

        if rdd.isEmpty() == False:
            collection = rdd.collect()
            result = list(zip(*collection)[1])
            spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master', kuduMasters)\
                 .option('kudu.table', kuduTableName).load().registerTempTable(kuduTableName)
            # insert into default.jira_events values (uuid(), localtimestamp, '')
            str = ''.join(collection[0][1])
            spark.sql("INSERT INTO TABLE {table} from (select uuid(), current_timestamp(), '{payload}')".format(
                table=kuduTableName, payload=result[0]))
            # spark.sql("INSERT INTO TABLE `" + kuduTableName +
            #           "` values (uuid(), `" + unix_timestamp() +`",`" + str + "`)")

            # PySpark KuduContext not yet available (https://issues.apache.org/jira/browse/KUDU-1603)

    windowedStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
