from kafka import KafkaConsumer, KafkaProducer
from mock_fleet_manager import Task

import random

class bcolors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'

consumer = KafkaConsumer(
    "result",
    bootstrap_servers='localhost:9092'
)

for msg in consumer:
    # Task is assigned.
    task_str = msg.value.decode("utf-8")
    task = Task.get_task_obj(task_str)
    if task.status == "done":
        print(f"{bcolors.GREEN}" + task.present() + f"{bcolors.ENDC}")
    else:
        print(f"{bcolors.RED}" + task.present() + f"{bcolors.ENDC}")