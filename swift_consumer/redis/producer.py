from datetime import datetime
import json
from redis import Redis
from time import sleep, time
from multiprocessing import Pool, current_process

PRODUCERS = 1
TOPIC = "topic"

def serialize(x):
    # print(f"Serializing {x}")
    return json.dumps(x).encode("utf8")

def produce(id):
    print(f"Starting producer {id}")
    producer = Redis("192.168.1.33")

    counter = 1
    timer = time()
    try:
        while True:
            producer.xadd(TOPIC, {"category": 1, "value": f"abcd-{counter}"})
            if counter % 100000 == 0:
                print(f"{current_process().name} | Sent {counter} at {datetime.now().isoformat(' ')} in {time()-timer}s")
                timer = time()
            counter += 1
            # sleep(5)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    with Pool(PRODUCERS) as p:
        p.map(produce, range(PRODUCERS))