import json
from multiprocessing import current_process, Pool
from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from datetime import datetime
from time import time

CONSUMERS = 6
TOPIC = "topic"


def decode(x):
    # print(x)
    try:
        return json.loads(x.decode("UTF-8"))
    except UnicodeDecodeError:
        print("Error: ", x)
        raise
    # return x.decode("UTF-8")


def consume(id):
    print(f"Starting consumer {id}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="192.168.1.33",
        client_id=f"{id}",
        key_deserializer=decode,
        value_deserializer=decode,
        partition_assignment_strategy=[RoundRobinPartitionAssignor],
        auto_offset_reset="earliest"
    )

    counter = 1
    timer = time()
    for message in consumer:
        if counter % 100000 == 0:
            print(
                f"{current_process().name} | Received {counter} at {datetime.now().isoformat(' ')} in {time()-timer}s")
            timer = time()
        # print(message.key, message.value)
        counter += 1


if __name__ == "__main__":
    with Pool(CONSUMERS) as p:
        p.map(consume, range(CONSUMERS))