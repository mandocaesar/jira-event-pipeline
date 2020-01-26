from flask import Flask
from flask import request
from confluent_kafka import Producer
import sys

app = Flask(__name__)

broker = "localhost:9092"
topic = "jira-event"

# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {"bootstrap.servers": broker}

# Create Producer instance
p = Producer(**conf)
# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        sys.stderr.write("%% Message failed delivery: %s\n" % err)
    else:
        sys.stderr.write(
            "%% Message delivered to %s [%d] @ %d\n"
            % (msg.topic(), msg.partition(), msg.offset())
        )


@app.route("/", methods=["POST"])
def postJsonHandler():
    print(request.is_json)
    content = request.get_data()
    # print(content)
    try:
        # Produce line (without newline)
        p.produce(topic, content, callback=delivery_callback)

    except BufferError:
        sys.stderr.write(
            "%% Local producer queue is full (%d messages awaiting delivery): try again\n"
            % len(p)
        )

        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write("%% Waiting for %d deliveries\n" % len(p))
    p.flush()

    return "JSON posted"


app.run(host="0.0.0.0", port=8090)

