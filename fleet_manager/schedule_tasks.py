# Airport Robot Fleet - Mock Scheduler

from kafka import KafkaProducer
from time import sleep, time
from mock_fleet_manager import FleetManager

import logging

class bcolors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'

logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

fm = FleetManager()

while True:
    task = fm.assign_task()
    message = f'{task.id},{task.airport},{task.fleet},{task.area},{task.robot},{task.task},{task.priority},{task.status},{task.starting_time},{task.finishing_time}'
    producer.send("assign", bytes(message, encoding="utf8"))
    print(f"{bcolors.YELLOW}" + task.present() + f"{bcolors.ENDC}")
    
    sleep(1)

producer.flush()
producer.close()