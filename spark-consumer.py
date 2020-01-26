from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    sc = SparkContext(appName="PythonSparkConsumer")
    ssc = StreamingContext(sc, 2)
    #    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(
        ssc, ["hola"], {"metadata.broker.list": "localhost:9092"}
    )
    lines = kvs.map(lambda x: x[1])
    counts = (
        lines.flatMap(lambda line: line.split(""))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    counts.pprint()
    # print(lines)
    ssc.start()
    ssc.awaitTermination()
