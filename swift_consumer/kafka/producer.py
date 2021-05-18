from datetime import datetime
import json
from kafka import KafkaProducer
from time import sleep, time
from multiprocessing import Pool, current_process

PRODUCERS = 1
TOPIC = "topic"

def serialize(x):
    # print(f"Serializing {x}")
    return json.dumps(x).encode("utf8")

def produce(id):
    print(f"Starting producer {id}")
    producer = KafkaProducer(bootstrap_servers="192.168.1.33:9092",
                            key_serializer=serialize,
                            value_serializer=serialize, client_id=str(id))

    counter = 1
    timer = time()
    try:
        while True:
            producer.send(TOPIC, value={"category": 1, "value": f"abcd-{counter}"}, key=f"{counter}")
            if counter % 100000 == 0:
                print(f"{current_process().name} | Sent {counter} at {datetime.now().isoformat(' ')} in {time()-timer}s")
                timer = time()
            counter += 1
            # sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    with Pool(PRODUCERS) as p:
        p.map(produce, range(PRODUCERS))