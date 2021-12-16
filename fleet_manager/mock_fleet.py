from time import sleep, time
from kafka import KafkaConsumer, KafkaProducer
from mock_fleet_manager import Task

import random

class bcolors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'

consumer = KafkaConsumer(
    "assign",
    bootstrap_servers='localhost:9092'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)
for msg in consumer:
    # Task is assigned.
    task_str = msg.value.decode("utf-8")
    task = Task.get_task_obj(task_str)
    print(f"{bcolors.YELLOW}" + task.present() + f"{bcolors.ENDC}")

    # Task is processing...
    wait_for = random.randint(1,9)/10
    sleep(wait_for)
    
    success = random.randint(0,10)
    if success == 0:
        task.status = "failed"
    else:
        task.status = "done"

    task.finishing_time = time()

    # Robot is sending a result to fleet manager.

    message = f'{task.id},{task.airport},{task.fleet},{task.area},{task.robot},{task.task},{task.priority},{task.status},{task.starting_time},{task.finishing_time}'
    producer.send("result", bytes(message, encoding="utf8"))
    if task.status == "done":
        print(f"{bcolors.GREEN}" + task.present() + f"{bcolors.ENDC}")
    else:
        print(f"{bcolors.RED}" + task.present() + f"{bcolors.ENDC}")
