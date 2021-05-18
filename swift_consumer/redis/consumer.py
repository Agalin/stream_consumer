import json
from multiprocessing import current_process, Pool
from redis import Redis
from datetime import datetime
from time import time

CONSUMERS = 1
TOPIC = "topic"
GROUP_NAME = "group"

def consume(id):
    print(f"Starting consumer {id}")
    consumer = Redis("192.168.1.33", decode_responses=True)
    
    counter = 1
    timer = time()
    while True:
        for _, messages in consumer.xreadgroup(GROUP_NAME, id, {TOPIC:">"}):
            for message in messages: 
                if counter % 100000 == 0:
                    print(
                        f"{current_process().name} | Received {counter} at {datetime.now().isoformat(' ')} in {time()-timer}s")
                    timer = time()
                # print(message[0], message[1])
                counter += 1


if __name__ == "__main__":
    r = Redis("192.168.1.33")
    r.xgroup_destroy(TOPIC, GROUP_NAME)
    r.xgroup_create(TOPIC, GROUP_NAME, mkstream=True)
    with Pool(CONSUMERS) as p:
        p.map(consume, range(CONSUMERS))